// Runs before every store handle is opened. Replace this file without
// rebuilding storage; the next open uses the new initialization function.
function configureStore(store, engines) {
  const MiB = 1024 * 1024;

  switch (store.type) {
    case "sql":
    case "column":
    case "vector":
      engines.sqlite.configure({
        cacheKiB: 8 * 1024,
        busyTimeoutMs: 5_000,
        tempStore: "file",
      });
      return;

    case "kv":
      engines.kv.configure({
        mapLowerMiB: 1,
        mapNowMiB: 16,
        mapUpperMiB: 512,
        mapGrowthMiB: 16,
        mapShrinkMiB: 16,
        autoCompactOnOpen: true,
      });
      return;

    case "files":
      engines.files.configure({ maxReadBytes: 32 * MiB });
      return;

    case "graph":
      engines.graph.configure({
        bufferPoolBytes: 64 * MiB,
        maxDbBytes: 2 * 1024 * MiB,
        maxThreads: 2,
        autoCheckpoint: true,
        checkpointThresholdBytes: 16 * MiB,
      });
      return;
  }
}
