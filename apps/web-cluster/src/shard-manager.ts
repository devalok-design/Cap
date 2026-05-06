import { RunnerAddress } from "@effect/cluster";
import {
	NodeClusterShardManagerSocket,
	NodeRuntime,
} from "@effect/platform-node";
import { Layer, Logger } from "effect";

import { DatabaseLive, ShardDatabaseLive } from "./shared/database.ts";

NodeClusterShardManagerSocket.layer({
	storage: "sql",
	shardingConfig: {
		shardManagerAddress: RunnerAddress.make({
			host: "0.0.0.0",
			port: 8080,
		}),
	},
}).pipe(
	Layer.provide(ShardDatabaseLive),
	Layer.provide(DatabaseLive),
	Layer.provide(Logger.pretty),
	Layer.launch,
	NodeRuntime.runMain,
);
