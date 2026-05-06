import { promises as fs } from "node:fs";
import { db } from "@cap/database";
import {
	organizations,
	s3Buckets,
	users,
	videos,
	videoUploads,
} from "@cap/database/schema";
import type { VideoMetadata } from "@cap/database/types";
import { serverEnv } from "@cap/env";
import { userIsPro } from "@cap/utils";
import { S3Buckets } from "@cap/web-backend";
import type { S3Bucket, Video } from "@cap/web-domain";
import { createClient } from "@deepgram/sdk";
import { eq } from "drizzle-orm";
import { Option } from "effect";
import {
	checkHasAudioTrack,
	extractAudioFromUrl,
} from "@/lib/audio-extract";
import {
	checkHasAudioTrackViaMediaServer,
	extractAudioViaMediaServer,
	isMediaServerConfigured,
	probeVideoViaMediaServer,
} from "@/lib/media-client";
import { runPromise } from "@/lib/server";
import { type DeepgramResult, formatToWebVTT } from "@/lib/transcribe-utils";
import { startAiGeneration } from "./generate-ai";

type TranscribeResult = {
	success: boolean;
	message: string;
};

export async function transcribeVideo(
	videoId: Video.VideoId,
	userId: string,
	aiGenerationEnabled = false,
	_isRetry = false,
): Promise<TranscribeResult> {
	if (!serverEnv().DEEPGRAM_API_KEY) {
		return {
			success: false,
			message: "Missing necessary environment variables",
		};
	}

	if (!userId || !videoId) {
		return {
			success: false,
			message: "userId or videoId not supplied",
		};
	}

	const query = await db()
		.select({
			video: videos,
			bucket: s3Buckets,
			settings: videos.settings,
			orgSettings: organizations.settings,
		})
		.from(videos)
		.leftJoin(s3Buckets, eq(videos.bucket, s3Buckets.id))
		.leftJoin(organizations, eq(videos.orgId, organizations.id))
		.where(eq(videos.id, videoId));

	if (query.length === 0) {
		return { success: false, message: "Video does not exist" };
	}

	const result = query[0];
	if (!result || !result.video) {
		return { success: false, message: "Video information is missing" };
	}

	const { video } = result;

	if (!video) {
		return { success: false, message: "Video information is missing" };
	}

	if (
		video.settings?.disableTranscript ??
		result.orgSettings?.disableTranscript
	) {
		console.log(
			`[transcribeVideo] Transcription disabled for video ${videoId}`,
		);
		try {
			await db()
				.update(videos)
				.set({ transcriptionStatus: "SKIPPED" })
				.where(eq(videos.id, videoId));
		} catch (err) {
			console.error(`[transcribeVideo] Failed to mark as skipped:`, err);
			return {
				success: false,
				message: "Transcription disabled, but failed to update status",
			};
		}
		return {
			success: true,
			message: "Transcription disabled for video — skipping transcription",
		};
	}

	if (
		video.transcriptionStatus === "COMPLETE" ||
		video.transcriptionStatus === "PROCESSING" ||
		video.transcriptionStatus === "SKIPPED" ||
		video.transcriptionStatus === "NO_AUDIO"
	) {
		return {
			success: true,
			message: "Transcription already completed or in progress",
		};
	}

	const upload = await db()
		.select({
			phase: videoUploads.phase,
			uploaded: videoUploads.uploaded,
			total: videoUploads.total,
		})
		.from(videoUploads)
		.where(eq(videoUploads.videoId, videoId))
		.limit(1);

	if (
		(upload[0]?.phase === "uploading" &&
			(upload[0]?.uploaded ?? 0) < (upload[0]?.total ?? 1)) ||
		upload[0]?.phase === "processing" ||
		upload[0]?.phase === "generating_thumbnail"
	) {
		return {
			success: true,
			message: "Video upload is still in progress",
		};
	}

	try {
		console.log(
			`[transcribeVideo] Triggering transcription for video ${videoId}`,
		);

		// Mark PROCESSING immediately so polling stops while transcription runs.
		// We bypass workflow/api start() — it silently no-ops on Railway
		// (needs VERCEL_URL for self-callbacks) and blocks direct invocation.
		await db()
			.update(videos)
			.set({ transcriptionStatus: "PROCESSING" })
			.where(eq(videos.id, videoId));

		runTranscriptionDirect(videoId, userId, aiGenerationEnabled).catch(
			(error) => {
				console.error(
					`[transcribeVideo] Transcription failed for ${videoId}:`,
					error,
				);
				db()
					.update(videos)
					.set({ transcriptionStatus: null })
					.where(eq(videos.id, videoId))
					.catch(() => {});
			},
		);

		return {
			success: true,
			message: "Transcription started",
		};
	} catch (error) {
		console.error("[transcribeVideo] Failed to start transcription:", error);

		await db()
			.update(videos)
			.set({ transcriptionStatus: null })
			.where(eq(videos.id, videoId));

		return {
			success: false,
			message: "Failed to start transcription",
		};
	}
}

async function runTranscriptionDirect(
	videoId: Video.VideoId,
	userId: string,
	aiGenerationEnabled: boolean,
): Promise<void> {
	console.log(`[transcribe] Starting direct transcription for ${videoId}`);

	// --- resolve bucket ---
	const videoQuery = await db()
		.select({
			video: videos,
			bucket: s3Buckets,
			owner: users,
		})
		.from(videos)
		.leftJoin(s3Buckets, eq(videos.bucket, s3Buckets.id))
		.innerJoin(users, eq(videos.ownerId, users.id))
		.where(eq(videos.id, videoId));

	if (!videoQuery[0]?.video) {
		throw new Error(`Video ${videoId} not found`);
	}

	const bucketId = (videoQuery[0].bucket?.id ?? null) as S3Bucket.S3BucketId | null;
	const isOwnerPro = userIsPro(videoQuery[0].owner);

	console.log(
		`[transcribe] Owner check: isOwnerPro=${isOwnerPro}`,
	);

	const [bucket] = await S3Buckets.getBucketAccess(
		Option.fromNullable(bucketId),
	).pipe(runPromise);

	// --- resolve video source URL ---
	const uploadRow = await db()
		.select({ rawFileKey: videoUploads.rawFileKey })
		.from(videoUploads)
		.where(eq(videoUploads.videoId, videoId))
		.limit(1);

	const candidateKeys = [
		`${userId}/${videoId}/result.mp4`,
		uploadRow[0]?.rawFileKey,
	].filter(
		(v, i, arr): v is string => Boolean(v) && arr.indexOf(v) === i,
	);

	let videoUrl: string | null = null;
	for (const key of candidateKeys) {
		const url = await bucket.getInternalSignedObjectUrl(key).pipe(runPromise);
		const probe = await fetch(url, { method: "GET", headers: { range: "bytes=0-0" } });
		if (probe.ok) {
			console.log(`[transcribe] Using video source ${key}`);
			videoUrl = url;
			break;
		}
	}

	if (!videoUrl) {
		throw new Error(`Video file not accessible for ${videoId}`);
	}

	// --- check / extract audio ---
	const useMediaServer = isMediaServerConfigured();
	console.log(`[transcribe] Audio detection: useMediaServer=${useMediaServer}, videoId=${videoId}`);

	let hasAudio: boolean;
	let audioBuffer: Buffer;

	if (useMediaServer) {
		try {
			const probe = await probeVideoViaMediaServer(videoUrl);
			console.log(
				`[transcribe] Probe: audioCodec=${probe.audioCodec}, videoCodec=${probe.videoCodec}, duration=${probe.duration}`,
			);
			hasAudio = probe.audioCodec !== null;
		} catch (probeError) {
			console.error(`[transcribe] Probe failed, falling back:`, probeError);
			hasAudio = await checkHasAudioTrackViaMediaServer(videoUrl);
		}

		if (!hasAudio) {
			console.log(`[transcribe] No audio track for ${videoId}`);
			await db()
				.update(videos)
				.set({ transcriptionStatus: "NO_AUDIO" })
				.where(eq(videos.id, videoId));
			return;
		}

		audioBuffer = await extractAudioViaMediaServer(videoUrl);
	} else {
		hasAudio = await checkHasAudioTrack(videoUrl);
		console.log(`[transcribe] Local ffmpeg audio check: hasAudio=${hasAudio}`);

		if (!hasAudio) {
			await db()
				.update(videos)
				.set({ transcriptionStatus: "NO_AUDIO" })
				.where(eq(videos.id, videoId));
			return;
		}

		const result = await extractAudioFromUrl(videoUrl);
		try {
			audioBuffer = await fs.readFile(result.filePath);
		} finally {
			await result.cleanup();
		}
	}

	console.log(`[transcribe] Extracted audio: ${audioBuffer.length} bytes`);

	// --- upload temp audio to S3 ---
	const audioKey = `${userId}/${videoId}/audio-temp.mp3`;
	await bucket.putObject(audioKey, audioBuffer, { contentType: "audio/mpeg" }).pipe(runPromise);
	const audioSignedUrl = await bucket.getInternalSignedObjectUrl(audioKey).pipe(runPromise);

	// --- transcribe with Deepgram ---
	console.log(`[transcribe] Sending audio to Deepgram for ${videoId}`);
	const audioResponse = await fetch(audioSignedUrl);
	if (!audioResponse.ok) {
		throw new Error(`Audio URL not accessible: ${audioResponse.status}`);
	}

	const audioBuf = Buffer.from(await audioResponse.arrayBuffer());
	const deepgram = createClient(serverEnv().DEEPGRAM_API_KEY as string);

	const { result: dgResult, error: dgError } =
		await deepgram.listen.prerecorded.transcribeFile(audioBuf, {
			model: "nova-3",
			smart_format: true,
			detect_language: true,
			utterances: true,
			mime_type: "audio/mpeg",
		});

	if (dgError) {
		throw new Error(`Deepgram failed: ${dgError.message}`);
	}

	const vtt = formatToWebVTT(dgResult as unknown as DeepgramResult);

	// --- save VTT + mark COMPLETE ---
	await bucket
		.putObject(`${userId}/${videoId}/transcription.vtt`, vtt, { contentType: "text/vtt" })
		.pipe(runPromise);

	await db()
		.update(videos)
		.set({ transcriptionStatus: "COMPLETE" })
		.where(eq(videos.id, videoId));

	console.log(`[transcribe] Transcription COMPLETE for ${videoId}`);

	// --- cleanup temp audio ---
	try {
		await bucket.deleteObject(audioKey).pipe(runPromise);
	} catch {
		console.error(`[transcribe] Failed to cleanup ${audioKey}`);
	}

	// --- queue AI generation if enabled ---
	if (aiGenerationEnabled) {
		await startAiGeneration(videoId, userId);
	}
}
