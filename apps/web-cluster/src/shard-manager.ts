import {
	NodeClusterShardManagerSocket,
	NodeRuntime,
} from "@effect/platform-node";
import { Layer, Logger } from "effect";

import { DatabaseLive, ShardDatabaseLive } from "./shared/database.ts";

const host = process.env.SHARD_MANAGER_HOST ?? "0.0.0.0";

NodeClusterShardManagerSocket.layer({
	storage: "sql",
	host,
}).pipe(
	Layer.provide(ShardDatabaseLive),
	Layer.provide(DatabaseLive),
	Layer.provide(Logger.pretty),
	Layer.launch,
	NodeRuntime.runMain,
);
