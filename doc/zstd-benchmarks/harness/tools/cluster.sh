#!/bin/bash
# cluster.sh start <frontend variant> [shard variant]   |   cluster.sh stop
# Env: FRONTEND_ZSTD FRONTEND_SHARD_ZSTD FE_SKIP_VERIFY FE_REUSE SHARD_REUSE
set -euo pipefail
S=${S:-$(pwd)}
RUN=$S/run
CFG=$S/tools/config
BIN=${BIN:-$S/../bin}
stop() {
  if [ -d $RUN ]; then
    for p in $RUN/*.pid; do [ -f "$p" ] && kill "$(cat $p)" 2>/dev/null || true; done
    sleep 0.5
    for p in $RUN/*.pid; do [ -f "$p" ] && kill -9 "$(cat $p)" 2>/dev/null || true; done
  fi
  rm -rf $RUN/data $RUN/*.pid
}
wait_port() { for _ in $(seq 150); do (exec 3<>/dev/tcp/127.0.0.1/$1) 2>/dev/null && return 0; sleep 0.1; done; echo "port $1 not up" >&2; return 1; }
case "$1" in
stop) stop ;;
start)
  stop
  VARIANT=$2 SHARD_VARIANT=${3:-base}
  mkdir -p $RUN/logs
  export DATA_DIR=$RUN/data TLS_DIR=$S/tls FRONTEND_ZSTD=${FRONTEND_ZSTD:-1} FRONTEND_SHARD_ZSTD=${FRONTEND_SHARD_ZSTD:-1}
  export FE_SKIP_VERIFY=${FE_SKIP_VERIFY:-0} FE_REUSE=${FE_REUSE:-0} SHARD_REUSE=${SHARD_REUSE:-0}
  for i in 0 1; do
    for d in cas/persistent_state_cdc ac/persistent_state_cdc cms/persistent_state; do mkdir -p $DATA_DIR/storage-$i/$d; done
    SHARD=$i $BIN/bb_storage-$SHARD_VARIANT $CFG/storage.jsonnet > $RUN/logs/storage-$i.log 2>&1 &
    echo $! > $RUN/storage-$i.pid
  done
  wait_port 18981; wait_port 18991
  for i in 0 1; do
    SHARD=$i $BIN/bb_storage-$SHARD_VARIANT $CFG/storage-cms.jsonnet > $RUN/logs/cms-$i.log 2>&1 &
    echo $! > $RUN/cms-$i.pid
  done
  wait_port 18985; wait_port 18995
  $BIN/bb_storage-$VARIANT $CFG/frontend.jsonnet > $RUN/logs/frontend.log 2>&1 &
  echo $! > $RUN/frontend.pid
  wait_port 18980
  ;;
esac
