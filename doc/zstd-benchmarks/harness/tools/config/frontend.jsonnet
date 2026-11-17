// Mirrors prod frontend.jsonnet's CAS/AC wiring. TLS like prod (self-signed),
// no JWT, no scheduler. supportedCompressors comes from FRONTEND_ZSTD.
local common = import 'common.libsonnet';
// EXPERIMENT (a): FRONTEND_SHARD_ZSTD=0 turns off enableCompression on the
// frontend's own shard clients only (workers/CMS/portal keep it).
local hopZstd = std.extVar('FRONTEND_SHARD_ZSTD') != '0';
local csExtra = if std.extVar('FE_SKIP_VERIFY') == '1' then { skipChunkDigestVerification: true } else {};
local cs = common.shardsOn(18981, hopZstd, csExtra);
local cms = common.shardsOn(18985, hopZstd);
{
  grpcServers: [{
    listenAddresses: ['127.0.0.1:18980'],
    keepaliveEnforcementPolicy: { minTime: '20s', permitWithoutStream: true },
    tls: {
      serverKeyPair: {
        files: {
          certificatePath: std.extVar('TLS_DIR') + '/tls.crt',
          privateKeyPath: std.extVar('TLS_DIR') + '/tls.key',
          refreshInterval: '86401s',
        },
      },
    },
    authenticationPolicy: { allow: {} },
  }],
  maximumMessageSizeBytes: common.maximumMessageSizeBytes,
  [if std.extVar('FRONTEND_ZSTD') == '1' then 'supportedCompressors']: ['ZSTD'],
  global: common.global(19980),
  contentAddressableStorageServer: {
    contentAddressableStorage: common.blobstore.contentAddressableStorage {
      chunkStorage: {
        existenceCaching: {
          backend: cs,
          existenceCache: { cacheSize: 1048576, cacheDuration: '300s', cacheReplacementPolicy: 'LEAST_RECENTLY_USED' },
        },
      },
      chunkMappingStorage: {
        existenceCaching: {
          backend: cms,
          existenceCache: { cacheSize: 131072, cacheDuration: '300s', cacheReplacementPolicy: 'LEAST_RECENTLY_USED' },
        },
      },
      chunkMappingCache: common.chunkMappingCache,
    },
    maximumChunkCount: common.maximumChunkCount,
    [if std.extVar('FE_REUSE') == '1' then 'reuseClientZstdFrames']: true,
    getAuthorizer: { allow: {} },
    putAuthorizer: { allow: {} },
    findMissingAuthorizer: { allow: {} },
  },
  actionCache: {
    backend: common.blobstore.actionCache,
    getAuthorizer: { allow: {} },
    putAuthorizer: { allow: {} },
  },
}
