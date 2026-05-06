import { db } from "@cap/database";
import { videos, videoUploads } from "@cap/database/schema";
import { serverEnv } from "@cap/env";
import { S3Buckets } from "@cap/web-backend";
import type { S3Bucket, Video } from "@cap/web-domain";
import { and, eq, ne } from "drizzle-orm";
import { Option } from "effect";
import { runPromise } from "@/lib/server";

export type VideoProcessingStartStatus = "started" | "already-processing";

const getAffectedRows = (result: unknown) => {
	if (Array.isArray(result)) {
		return (
			(result[0] as { affectedRows?: number } | undefined)?.affectedRows ?? 0
		);
	}

	return (result as { affectedRows?: number } | undefined)?.affectedRows ?? 0;
};

export async function setVideoProcessingError(
	videoId: Video.VideoId,
	processingMessage: string,
	error: unknown,
): Promise<void> {
	await db()
		.update(videoUploads)
		.set({
			phase: "error",
			processingProgress: 0,
			processingMessage,
			processingError: error instanceof Error ? error.message : String(error),
			updatedAt: new Date(),
		})
		.where(eq(videoUploads.videoId, videoId));
}

export async function transitionVideoToProcessing({
	videoId,
	rawFileKey,
	processingMessage,
	mode,
	forceRestart,
}: {
	videoId: Video.VideoId;
	rawFileKey: string;
	processingMessage: string;
	mode?: "singlepart" | "multipart";
	forceRestart?: boolean;
}): Promise<VideoProcessingStartStatus> {
	const result = await db()
		.update(videoUploads)
		.set({
			...(mode ? { mode } : {}),
			phase: "processing",
			processingProgress: 0,
			processingMessage,
			processingError: null,
			rawFileKey,
			updatedAt: new Date(),
		})
		.where(
			forceRestart
				? eq(videoUploads.videoId, videoId)
				: and(
						eq(videoUploads.videoId, videoId),
						ne(videoUploads.phase, "processing"),
					),
		);

	if (getAffectedRows(result) > 0) {
		return "started";
	}

	const [upload] = await db()
		.select()
		.from(videoUploads)
		.where(eq(videoUploads.videoId, videoId));

	if (!upload) {
		throw new Error("No upload record found");
	}

	if (upload.phase === "processing") {
		return "already-processing";
	}

	throw new Error("Failed to transition upload to processing");
}

function getInputExtension(rawFileKey: string): string {
	const parts = rawFileKey.split(".");
	const extension = parts.at(-1)?.toLowerCase();
	return extension ? `.${extension}` : ".mp4";
}

function getValidDuration(duration: number) {
	return Number.isFinite(duration) && duration > 0 ? duration : undefined;
}

const MEDIA_SERVER_START_MAX_ATTEMPTS = 6;
const MEDIA_SERVER_START_RETRY_BASE_MS = 2000;

async function startMediaServerProcessJob(
	mediaServerUrl: string,
	body: {
		videoId: string;
		userId: string;
		videoUrl: string;
		outputPresignedUrl: string;
		thumbnailPresignedUrl: string;
		webhookUrl: string;
		webhookSecret?: string;
		inputExtension: string;
	},
): Promise<string> {
	for (let attempt = 0; attempt < MEDIA_SERVER_START_MAX_ATTEMPTS; attempt++) {
		const headers: Record<string, string> = {
			"Content-Type": "application/json",
		};
		if (body.webhookSecret) {
			headers["x-media-server-secret"] = body.webhookSecret;
		}

		const response = await fetch(`${mediaServerUrl}/video/process`, {
			method: "POST",
			headers,
			body: JSON.stringify(body),
		});

		if (response.ok) {
			const { jobId } = (await response.json()) as { jobId: string };
			return jobId;
		}

		const errorData = (await response.json().catch(() => ({}))) as {
			error?: string;
			code?: string;
			details?: string;
		};
		const errorMessage =
			errorData.error || errorData.details || "Video processing failed to start";
		const shouldRetry =
			response.status === 503 &&
			(errorData.code === "SERVER_BUSY" ||
				errorMessage.includes("Server is busy"));

		if (shouldRetry && attempt < MEDIA_SERVER_START_MAX_ATTEMPTS - 1) {
			await new Promise((resolve) =>
				setTimeout(resolve, MEDIA_SERVER_START_RETRY_BASE_MS * 2 ** attempt),
			);
			continue;
		}

		throw new Error(errorMessage);
	}

	throw new Error("Video processing failed to start after retries");
}

async function pollForCompletion(
	mediaServerUrl: string,
	jobId: string,
): Promise<{ metadata: { duration: number; width: number; height: number; fps: number } }> {
	const maxAttempts = 360;
	const pollIntervalMs = 5000;

	for (let attempt = 0; attempt < maxAttempts; attempt++) {
		await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));

		const response = await fetch(
			`${mediaServerUrl}/video/process/${jobId}/status`,
			{ method: "GET", headers: { Accept: "application/json" } },
		);

		if (!response.ok) {
			console.warn(
				`[video-processing] Poll failed: ${response.status} for job ${jobId}`,
			);
			continue;
		}

		const status = (await response.json()) as {
			phase: string;
			progress: number;
			error?: string;
			metadata?: { duration: number; width: number; height: number; fps: number };
		};

		if (status.phase === "complete") {
			if (!status.metadata) throw new Error("Processing complete but no metadata");
			return { metadata: status.metadata };
		}
		if (status.phase === "error") throw new Error(status.error || "Video processing failed");
		if (status.phase === "cancelled") throw new Error("Video processing cancelled");
	}

	throw new Error("Video processing timed out");
}

async function runVideoProcessingDirect(params: {
	videoId: Video.VideoId;
	userId: string;
	rawFileKey: string;
	bucketId: string | null;
}): Promise<void> {
	const { videoId, userId, rawFileKey, bucketId } = params;

	console.log(`[video-processing] Starting direct processing for ${videoId}`);

	const mediaServerUrl = serverEnv().MEDIA_SERVER_URL;
	if (!mediaServerUrl) {
		console.error("[video-processing] MEDIA_SERVER_URL not configured");
		await setVideoProcessingError(
			videoId,
			"Media server not configured",
			new Error("MEDIA_SERVER_URL not configured"),
		);
		return;
	}

	try {
		const [bucket] = await S3Buckets.getBucketAccess(
			Option.fromNullable(bucketId as S3Bucket.S3BucketId | null),
		).pipe(runPromise);

		const rawVideoUrl = await bucket
			.getInternalSignedObjectUrl(rawFileKey)
			.pipe(runPromise);

		const outputKey = `${userId}/${videoId}/result.mp4`;
		const thumbnailKey = `${userId}/${videoId}/screenshot/screen-capture.jpg`;

		const outputPresignedUrl = await bucket
			.getInternalPresignedPutUrl(outputKey, { ContentType: "video/mp4" })
			.pipe(runPromise);

		const thumbnailPresignedUrl = await bucket
			.getInternalPresignedPutUrl(thumbnailKey, { ContentType: "image/jpeg" })
			.pipe(runPromise);

		const webhookBaseUrl =
			serverEnv().MEDIA_SERVER_WEBHOOK_URL || serverEnv().WEB_URL;
		const webhookUrl = `${webhookBaseUrl}/api/webhooks/media-server/progress`;
		const webhookSecret = serverEnv().MEDIA_SERVER_WEBHOOK_SECRET;

		const jobId = await startMediaServerProcessJob(mediaServerUrl, {
			videoId,
			userId,
			videoUrl: rawVideoUrl,
			outputPresignedUrl,
			thumbnailPresignedUrl,
			webhookUrl,
			webhookSecret: webhookSecret || undefined,
			inputExtension: getInputExtension(rawFileKey),
		});

		console.log(
			`[video-processing] Media server job ${jobId} started for ${videoId}`,
		);

		const result = await pollForCompletion(mediaServerUrl, jobId);

		// Save metadata and delete upload row
		const duration = getValidDuration(result.metadata.duration);
		await db()
			.update(videos)
			.set({
				width: result.metadata.width,
				height: result.metadata.height,
				fps: result.metadata.fps,
				...(duration === undefined ? {} : { duration }),
			})
			.where(eq(videos.id, videoId));

		await db()
			.delete(videoUploads)
			.where(eq(videoUploads.videoId, videoId));

		// Delete raw upload from S3
		try {
			const [cleanupBucket] = await S3Buckets.getBucketAccess(
				Option.fromNullable(bucketId as S3Bucket.S3BucketId | null),
			).pipe(runPromise);
			await cleanupBucket.deleteObject(rawFileKey).pipe(runPromise);
		} catch (cleanupErr) {
			console.error(
				`[video-processing] Failed to delete raw upload for ${videoId}:`,
				cleanupErr,
			);
		}

		console.log(`[video-processing] Direct processing COMPLETE for ${videoId}`);
	} catch (error) {
		console.error(
			`[video-processing] Direct processing ERROR for ${videoId}:`,
			error,
		);
		await setVideoProcessingError(
			videoId,
			"Video processing failed",
			error instanceof Error ? error : new Error(String(error)),
		);
	}
}

export async function startVideoProcessingWorkflow({
	videoId,
	userId,
	rawFileKey,
	bucketId,
	processingMessage,
	mode,
	forceRestart,
}: {
	videoId: Video.VideoId;
	userId: string;
	rawFileKey: string;
	bucketId: string | null;
	processingMessage: string;
	startFailureMessage: string;
	mode?: "singlepart" | "multipart";
	forceRestart?: boolean;
}): Promise<VideoProcessingStartStatus> {
	const status = await transitionVideoToProcessing({
		videoId,
		rawFileKey,
		processingMessage,
		mode,
		forceRestart,
	});

	if (status === "already-processing") {
		return status;
	}

	runVideoProcessingDirect({ videoId, userId, rawFileKey, bucketId }).catch(
		(error) => {
			console.error(
				`[video-processing] Unhandled error for ${videoId}:`,
				error,
			);
		},
	);

	return "started";
}
