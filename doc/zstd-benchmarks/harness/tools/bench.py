#!/usr/bin/env python3
"""bench.py <cells.json> [RUNS]  -- runs each cell RUNS times on a fresh cluster, prints medians.
cell: {"name":..., "fe":"base", "shard":"base", "env":{...}, "client":["-batch_compress=1024", ...], "link": null|"a"|"b"}"""
import json, os, subprocess, sys, statistics, time, urllib.request
H = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.path.join(H, '..', 'bin')
cells = json.load(open(sys.argv[1]))
runs = int(sys.argv[2]) if len(sys.argv) > 2 else 5
outdir = os.path.join(H, 'results', os.path.splitext(os.path.basename(sys.argv[1]))[0])
os.makedirs(outdir, exist_ok=True)
LINKS = {'a': (2, 2000), 'b': (40, 300)}
summary = {}
for cell in cells:
    env = dict(os.environ, S=H, BIN=BIN, **cell.get('env', {}))
    subprocess.run([H + '/tools/cluster.sh', 'start', cell.get('fe', 'base'), cell.get('shard', 'base')], env=env, check=True)
    fe = open(H + '/run/frontend.pid').read().strip()
    sh = ','.join(open(H + '/run/%s.pid' % n).read().strip() for n in ('storage-0', 'storage-1', 'cms-0', 'cms-1'))
    proxy = None
    addr = '127.0.0.1:18980'
    extra = []
    if cell.get('link'):
        rtt, mbps = LINKS[cell['link']]
        proxy = subprocess.Popen([BIN + '/linkproxy', '-rtt_ms=%d' % rtt, '-mbps=%d' % mbps], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(0.5)
        addr = '127.0.0.1:28980'
        extra = ['-proxy_stats=http://127.0.0.1:28999/stats']
    res = []
    prof = cell.get('profile')
    for r in range(runs):
        out = os.path.join(outdir, '%s-run%d.json' % (cell['name'], r))
        procs = []
        if prof and r == runs - 1:
            for nm, port in (('frontend', 19980), ('shard0', 19981), ('shard1', 19991)):
                procs.append(subprocess.Popen(['curl', '-s', '-o', os.path.join(outdir, '%s-%s.pprof' % (cell['name'], nm)), 'http://127.0.0.1:%d/debug/pprof/profile?seconds=%d' % (port, prof)]))
            time.sleep(0.3)
        p = subprocess.run([BIN + '/casbench', '-addr', addr, '-tls_ca_cert', H + '/tls/tls.crt', '-corpus', H + '/' + cell.get('corpus', 'corpus.txt'),
                            '-salt', '%s-%d-%d' % (cell['name'], r, time.time_ns()), '-frontend_pid', fe, '-shard_pids', sh, '-out', out] + extra + cell.get('client', []),
                           capture_output=True, text=True)
        for q in procs: q.wait()
        open(out + '.log', 'w').write(p.stderr)
        res.append(json.load(open(out)))
    if proxy: proxy.kill()
    subprocess.run([H + '/tools/cluster.sh', 'stop'], env=env)
    med = {}
    for i, ph in enumerate(res[0]):
        g = lambda k: statistics.median(r[i][k] for r in res)
        wire = lambda d: statistics.median(sum(v[d] for v in r[i]['rpc'].values()) for r in res)
        med[ph['phase']] = dict(mb=ph['bytes'] / 1e6, wall=g('wall_s'), fe=g('frontend_cpu_s'), shard=g('shard_cpu_s'), client=g('client_cpu_s'),
                                sent_mb=wire('sent') / 1e6, recv_mb=wire('recv') / 1e6,
                                errors=sum(r[i]['errors'] for r in res), verified=min(r[i]['verified'] for r in res))
    summary[cell['name']] = med
    for ph, m in med.items():
        print('%-34s %-11s wall=%5.2fs fe=%5.2fs shard=%5.2fs client=%5.2fs sent=%6.1fMB recv=%6.1fMB err=%d ver=%d' % (cell['name'], ph, m['wall'], m['fe'], m['shard'], m['client'], m['sent_mb'], m['recv_mb'], m['errors'], m['verified']), flush=True)
json.dump(summary, open(os.path.join(outdir, 'summary.json'), 'w'), indent=1)
