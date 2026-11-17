// Mirrors prod storage.jsonnet (CS + AC on one port), sized down.
local common = import 'common.libsonnet';
local i = std.parseInt(std.extVar('SHARD'));
local dir = std.extVar('DATA_DIR') + '/storage-' + i;
{
  grpcServers: [{
    listenAddresses: ['127.0.0.1:%d' % common.shardPort(i, 18981)],
    authenticationPolicy: { allow: {} },
  }],
  maximumMessageSizeBytes: common.maximumMessageSizeBytes,
  supportedCompressors: ['ZSTD'],
  global: common.global(common.shardPort(i, 19981)),
  contentAddressableStorageServer: {
    contentAddressableStorage: {
      chunkStorage: {
        'local': {
          keyLocationMap: {
            onBlockDevice: { file: { path: dir + '/cas/key_location_map', sizeBytes: 64 * 1024 * 1024 } },
            maximumGetAttempts: 16,
            maximumPutAttempts: 64,
          },
          oldBlocks: 8,
          currentBlocks: 14,
          newBlocks: 5,
          blocksOnBlockDevice: {
            source: { file: { path: dir + '/cas/blocks', sizeBytes: 3 * 1024 * 1024 * 1024 } },
            spareBlocks: 3,
          },
          persistent: { stateDirectoryPath: dir + '/cas/persistent_state_cdc', minimumEpochInterval: '300s' },
          chunkingParameters: {
            minChunkSizeBytes: common.minChunkSizeBytes,
            horizonSizeBytes: 8 * common.minChunkSizeBytes,
          },
        },
      },
      chunkMappingStorage: {
        'error': { code: 12, message: 'Chunk mappings are served on the CMS port' },
      },
    },
    maximumChunkCount: common.maximumChunkCount,
    [if std.extVar('SHARD_REUSE') == '1' then 'reuseClientZstdFrames']: true,
    getAuthorizer: { allow: {} },
    putAuthorizer: { allow: {} },
    findMissingAuthorizer: { allow: {} },
  },
  actionCache: {
    backend: {
      'local': {
        keyLocationMap: {
          onBlockDevice: { file: { path: dir + '/ac/key_location_map', sizeBytes: 16 * 1024 * 1024 } },
          maximumGetAttempts: 32,
          maximumPutAttempts: 128,
        },
        oldBlocks: 8,
        currentBlocks: 24,
        newBlocks: 1,
        blocksOnBlockDevice: {
          source: { file: { path: dir + '/ac/blocks', sizeBytes: 64 * 1024 * 1024 } },
          spareBlocks: 3,
        },
        persistent: { stateDirectoryPath: dir + '/ac/persistent_state_cdc', minimumEpochInterval: '300s' },
      },
    },
    getAuthorizer: { allow: {} },
    putAuthorizer: { allow: {} },
  },
}
