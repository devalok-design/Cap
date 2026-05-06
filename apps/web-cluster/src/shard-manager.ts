import { RunnerAddress } from "@effect/cluster";
import {
	NodeClusterShardManagerSocket,
	NodeRuntime,
} from "@effect/platform-node";
import { Layer, Logger } from "effect";

import { DatabaseLive, ShardDatabaseLive } from "./shared/database.ts";

const shardManagerHost = Deno.env.get("SHARD_MANAGER_HOST") ?? "0.0.0.0";
const shardManagerAddress = `${shardManagerHost}:8080`;

NodeClusterShardManagerSocket.layer({
	storage: "sql",
	shardingConfig: {
		shardManagerAddress: RunnerAddress.make(shardManagerAddress),
	},
}).pipe(
	Layer.provide(ShardDatabaseLive),
	Layer.provide(DatabaseLive),
	Layer.provide(Logger.pretty),
	Layer.launch,
	NodeRuntime.runMain,
);
