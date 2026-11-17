// Local copy of electron/infra argo/rbe-cluster/buildbarn/config/common.libsonnet
// (origin/main 7d7019b): same structure, 2 shards instead of 5, no mTLS,
// no tracing, small block devices on files.
local nShards = 2;
local shardPort(i, port) = port + 10 * i;
local shardsOn(port, compress=true, extra={}) = {
  sharding: {
    shards: {
      [std.toString(i)]: {
        backend: {
          grpc: {
            client: { address: '127.0.0.1:%d' % shardPort(i, port) },
            enableCompression: compress,
          } + extra,
        },
        weight: 1,
      }
      for i in std.range(0, nShards - 1)
    },
  },
};
local shards = shardsOn(18981);
{
  nShards:: nShards,
  shardPort:: shardPort,
  shardsOn:: shardsOn,
  blobstore: {
    contentAddressableStorage: {
      chunkStorage: shards,
      chunkMappingStorage: shardsOn(18985),
      cdcParameterCache: {
        cacheSize: 1000,
        cacheDuration: '60s',
        cacheReplacementPolicy: 'LEAST_RECENTLY_USED',
      },
    },
    actionCache: {
      completenessChecking: {
        backend: shards,
        maximumTotalTreeSizeBytes: 64 * 1024 * 1024,
      },
    },
  },
  chunkMappingCache:: {
    cacheSize: 16384,
    cacheDuration: '300s',
    cacheReplacementPolicy: 'LEAST_RECENTLY_USED',
  },
  minChunkSizeBytes:: 1024 * 1024,
  maximumChunkCount:: 100000,
  maximumMessageSizeBytes: 4 * 1024 * 1024,
  global(diagPort):: {
    diagnosticsHttpServer: {
      httpServers: [{
        listenAddresses: ['127.0.0.1:%d' % diagPort],
        authenticationPolicy: { allow: {} },
      }],
      enablePrometheus: true,
      enablePprof: true,
    },
  },
}
