#!/usr/bin/env python3
"""Compare successful storage runs only when runtime and workload identities match."""

import argparse
import json
from pathlib import Path
import statistics

from storage_contract import summarize, verify


def comparable(left, right):
    if (left['package']['files'] != right['package']['files'] or left['image'] != right['image']
            or left['workload'] != right['workload']):
        raise ValueError('package, workload image or guest workload differs')


def load(directory):
    root = Path(directory)
    result = json.loads((root / 'result.json').read_text())
    verify(result)
    if result.get('diagnostic_only'):
        raise ValueError('traced diagnostic run is excluded from performance comparison')
    if result.get('meta_profile_enabled'):
        raise ValueError('Meta profiling run is excluded from performance comparison')
    cleanup = json.loads((root / 'storage-cleanup.json').read_text())
    if cleanup['exit_code'] != 0 or cleanup.get('errors') != []:
        raise ValueError('storage suite did not finish successfully')
    result['fixture_cleanup'] = cleanup
    identities = {'package': json.loads((root / 'package-manifest.json').read_text()),
                  'image': json.loads((root / 'execd-image.json').read_text()), 'workload': result['workload']}
    hashes = json.loads((root / 'runtime-hashes.json').read_text())
    identities['runtime'] = {Path(key).name: digest for key, digest in hashes.items()
                             if Path(key).name in ('sandboxd', 'firecracker', 'Image', 'initrd.img')}
    return result, identities


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('evidence', type=Path, nargs='+')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--markdown', type=Path)
    args = parser.parse_args()
    runs = [load(path) for path in args.evidence]
    if len(runs) < 2:
        raise ValueError('at least two successful backend runs are required')
    baseline = runs[0][1]
    for _, identity in runs[1:]:
        comparable(baseline, identity)
        if baseline['runtime'] != identity['runtime']:
            raise ValueError('external runtime identities differ')
    result = {'scope': 'one KVM host, two isolated execution nodes; cold peer / warm owner; no machine-wide cache flush',
              'identities': baseline, 'backends': []}
    for run, _ in runs:
        result['backends'].append({'storage': run['storage'], 'summary': summarize(run['samples'], run['repetitions']),
                                   'samples': run['samples'], 'fixture_cleanup': run['fixture_cleanup'],
                                   'lifecycle_cases': run['cases']})
    args.output.write_text(json.dumps(result, indent=2))
    if args.markdown:
        lines = ['# AFS / S3 Firecracker 存储验证', '',
                 '同一专用 KVM 主机、两个隔离执行节点、两个 Ingress；同一发布包、外部运行时、guest 镜像和工作负载。',
                 '每个 Environment 为 1 vCPU / 256 MiB，延迟访问工作集 32 MiB，快照逻辑大小约 288 MiB。',
                 '磁盘 I/O 使用 Sync / Writeback。冷恢复为独立节点、无该制品缓存；热恢复为原节点。未执行全机缓存清理。', '',
                 '每项恢复中位数取三次；暂停中位数取六次。每次 12 个请求经过两个 Ingress，只恢复一次，并校验 guest PID、内存状态及文件。', '',
                 '| 后端 | 暂停发布中位数 (s) | 冷恢复首 HTTP (s) | 热恢复首 HTTP (s) |',
                 '| --- | ---: | ---: | ---: |']
        for backend in result['backends']:
            samples = backend['samples']
            kind = backend['storage']['kind']
            label = {'afs': 'AFS', 'minio': 'S3 / MinIO', 'real_obs': 'OBS / ' + backend['storage'].get('region', '')}.get(kind, kind)
            pause = statistics.median(s['pause_seconds'] for s in samples)
            cold = statistics.median(s['first_http_seconds'] for s in samples if s['cache'] == 'cold')
            warm = statistics.median(s['first_http_seconds'] for s in samples if s['cache'] == 'warm')
            lines.append(f'| {label} | {pause:.3f} | {cold:.3f} | {warm:.3f} |')
        lines.extend(['', 'AFS 为 local-file Meta、三个客户端、单份必需持久副本；不代表存储 HA 或跨物理主机故障域。',
                      'MinIO 与 AFS 使用本机存储链路；OBS 包含远端网络成本，不能用这组结果分离纯后端性能。', '',
                      '## 生命周期与清理', ''])
        for backend in result['backends']:
            lines.append(f"- {backend['storage']['kind']}: {len(backend['lifecycle_cases'])} 项生命周期通过；执行节点最终 inventory 均为 0。")
            for error in backend['fixture_cleanup']['errors']:
                lines.append('  AFS 服务清理未通过：' + error)
        lines.extend(['', '原始样本、每阶段 AFS 读流量、VMM 映射及回收系统调用见同目录 JSON 和导出的 evidence。', ''])
        args.markdown.write_text('\n'.join(lines))


if __name__ == '__main__':
    main()
