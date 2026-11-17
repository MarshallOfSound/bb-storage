// Mirrors prod storage-cms.jsonnet (Chunk Mapping Storage, validated against
// the sharded chunk storage), sized down.
local common = import 'common.libsonnet';
local i = std.parseInt(std.extVar('SHARD'));
local dir = std.extVar('DATA_DIR') + '/storage-' + i;
{
  grpcServers: [{
    listenAddresses: ['127.0.0.1:%d' % common.shardPort(i, 18985)],
    authenticationPolicy: { allow: {} },
  }],
  maximumMessageSizeBytes: common.maximumMessageSizeBytes,
  supportedCompressors: ['ZSTD'],
  global: common.global(common.shardPort(i, 19985)),
  contentAddressableStorageServer: {
    contentAddressableStorage: common.blobstore.contentAddressableStorage {
      chunkStorage: {
        existenceCaching: {
          backend: common.blobstore.contentAddressableStorage.chunkStorage,
          existenceCache: { cacheSize: 262144, cacheDuration: '300s', cacheReplacementPolicy: 'LEAST_RECENTLY_USED' },
        },
      },
      chunkMappingStorage: {
        chunkMappingValidating: {
          backend: {
            'local': {
              keyLocationMap: {
                onBlockDevice: { file: { path: dir + '/cms/key_location_map', sizeBytes: 16 * 1024 * 1024 } },
                maximumGetAttempts: 32,
                maximumPutAttempts: 128,
              },
              oldBlocks: 8,
              currentBlocks: 24,
              newBlocks: 1,
              blocksOnBlockDevice: {
                source: { file: { path: dir + '/cms/blocks', sizeBytes: 64 * 1024 * 1024 } },
                spareBlocks: 3,
              },
              persistent: { stateDirectoryPath: dir + '/cms/persistent_state', minimumEpochInterval: '300s' },
            },
          },
        },
      },
    },
    getAuthorizer: { allow: {} },
    putAuthorizer: { allow: {} },
    findMissingAuthorizer: { allow: {} },
    maximumChunkCount: common.maximumChunkCount,
  },
}
