import re, subprocess, sys, os, ast
repo, mockgen = sys.argv[1], sys.argv[2]
src = open(os.path.join(repo, 'internal/mock/BUILD.bazel')).read()
LIBS = {
 '@bazel_remote_apis//build/bazel/remote/execution/v2:remote_execution_go_proto': 'github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2',
 '@com_github_prometheus_client_golang//prometheus': 'github.com/prometheus/client_golang/prometheus',
 '@org_golang_google_grpc//:grpc': 'google.golang.org/grpc',
 '@io_opentelemetry_go_otel_trace//:trace': 'go.opentelemetry.io/otel/trace',
 '@org_golang_google_genproto_googleapis_bytestream//:bytestream': 'google.golang.org/genproto/googleapis/bytestream',
}
def imp(label):
    if label in LIBS: return LIBS[label]
    if label.startswith('//'):
        return 'github.com/buildbarn/bb-storage/' + label[2:].split(':')[0]
    raise Exception(label)
fail = 0
for m in re.finditer(r'^gomock\((.*?)^\)', src, re.S | re.M):
    body = m.group(1)
    def attr(n):
        mm = re.search(r'^\s+%s = (.*?),\n(?=\s+\w+ = |\s*\Z)' % n, body + '\n', re.S | re.M)
        return ast.literal_eval(mm.group(1)) if mm else None
    out, ifaces, lib, source, names = attr('out'), attr('interfaces'), attr('library'), attr('source'), attr('mock_names')
    cmd = [mockgen, '-package', 'mock', '-destination', os.path.join(repo, 'internal/mock', out)]
    if names: cmd += ['-mock_names', ','.join('%s=%s' % kv for kv in names.items())]
    if source:
        p = source[2:].replace(':', '/')
        cmd += ['-source', p, ','.join(ifaces)] if False else ['-source', p]
    else:
        cmd += [imp(lib), ','.join(ifaces)]
    r = subprocess.run(cmd, cwd=repo, capture_output=True, text=True)
    if r.returncode:
        fail += 1
        print('FAIL', out, r.stderr[-600:])
print('failures', fail)
