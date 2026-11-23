"""Execution record: reuse the repository CLIs for the real-data benchmark."""

import json
import os
import platform
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[3]
DEST = Path(__file__).resolve().parent
ID = DEST.name
DOCKER_BIN = Path('C:/Program Files/Docker/Docker/resources/bin')
DOCKER = str(DOCKER_BIN / 'docker.exe')
ENV = {**os.environ, 'PATH': str(DOCKER_BIN) + os.pathsep + os.environ['PATH']}
COMPOSE = [DOCKER, 'compose', '--env-file', 'docker/.env', '-f', 'docker/docker-compose.yml']
CONTAINER_DIR = '/tmp/' + ID
BUCKET = 'bigdata-network'
SOURCE = f's3a://{BUCKET}/raw/cicids2017/'
SMALL = SOURCE + 'Friday-WorkingHours-Morning.pcap_ISCX.csv'
COMMON = ['--dataset-kind', 'real', '--experiment-id', ID, '--warmups', '1',
          '--repeats', '3', '--adaptive', 'false', '--master-ui', 'http://spark-master:8080',
          '--output-prefix', f's3a://{BUCKET}/benchmarks/{ID}', '--execute']
COMMANDS = DEST / 'execution.jsonl'


def command(name, args):
    log_path = DEST / (name + '.log')
    started = datetime.now(timezone.utc).isoformat()
    print(f'START {name}', flush=True)
    with log_path.open('x', encoding='utf-8') as log:
        process = subprocess.Popen(args, cwd=ROOT, env=ENV, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, encoding='utf-8', errors='replace')
        for line in process.stdout:
            log.write(line)
            log.flush()
            if line.startswith('{"dimension":') or line.startswith('Uploaded '):
                print(line.rstrip(), flush=True)
        code = process.wait()
    with COMMANDS.open('a', encoding='utf-8') as out:
        out.write(json.dumps({'started_at': started, 'finished_at': datetime.now(timezone.utc).isoformat(),
                              'name': name, 'args': args, 'exit_code': code}) + '\n')
    print(f'END {name}: exit={code}', flush=True)
    if code:
        raise RuntimeError(f'{name} failed: exit={code}; see {log_path.name}')


def topology(expected):
    for _ in range(30):
        with urlopen('http://localhost:8080/json/', timeout=5) as response:
            master = json.load(response)
        live = [worker for worker in master['workers'] if worker['state'] == 'ALIVE']
        if len(live) == expected and not master.get('activeapps'):
            return master
        time.sleep(2)
    raise RuntimeError(f'Expected {expected} ALIVE workers and no other active applications')


def bench(name, module, extra, workers):
    topology(workers)
    command(name, COMPOSE + ['exec', '-T', '-e', 'SPARK_EXECUTOR_MEMORY=2g',
            'spark-master', 'python3', '-u', '-m', 'experiments.' + module] + extra +
            ['--expected-workers', str(workers), '--results', CONTAINER_DIR + '/' + name + '.json'] + COMMON)
    check = subprocess.check_output(COMPOSE + ['exec', '-T', 'spark-master', 'python3', '-c',
        "import json; j=json.load(open('" + CONTAINER_DIR + '/' + name + ".json')); "
        "assert j['run_status']=='COMPLETE'; print(len(j['runs']),len(j['warmup_runs']))"],
        cwd=ROOT, env=ENV, text=True)
    print(f'VERIFIED {name}: {check.strip()} measured/warmup', flush=True)
    command('copy-' + name, [DOCKER, 'cp', 'spark-master:' + CONTAINER_DIR + '/' + name + '.json',
                             str(DEST / (name + '.json'))])


def main():
    if COMMANDS.exists():
        raise FileExistsError('Execution record exists; do not overwrite an earlier experiment')
    audit = json.loads((DEST / 'source-audit.json').read_text(encoding='utf-8'))
    if not audit['structurally_ready_for_upload']:
        raise ValueError('Source audit failed')
    topology(2)
    info = json.loads(subprocess.check_output([DOCKER, 'info', '--format', '{{json .}}'],
                                              cwd=ROOT, env=ENV, text=True))
    environment = {'timestamp_utc': datetime.now(timezone.utc).isoformat(),
        'host_platform': platform.platform(), 'host_python': platform.python_version(),
        'docker': {key: info.get(key) for key in ['ServerVersion', 'OperatingSystem', 'OSType',
                    'Architecture', 'KernelVersion', 'NCPU', 'MemTotal', 'Driver']},
        'git_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        'initial_topology': topology(2),
        'selection_note': 'Nested original files: Friday Morning; all Friday; all eight CSVs. Label mix changes with size.'}
    with (DEST / 'environment.json').open('x', encoding='utf-8') as out:
        json.dump(environment, out, indent=2)
    upload = COMPOSE + ['exec', '-T', 'spark-master', 'python3', '-m', 'storage.upload_minio',
            '--source-dir', '/workspace/79rows', '--audit-report',
            '/workspace/docs/benchmarks/' + ID + '/source-audit.json']
    command('upload-dry-run', upload)
    command('upload', upload + ['--execute'])
    bench('size', 'benchmark_size', ['--inputs', SMALL, SOURCE + 'Friday*.csv', SOURCE + '*.csv'], 2)
    bench('partitions', 'benchmark_partitions', ['--input', SMALL, '--partitions', '4', '8', '16', '32'], 2)
    topology(2)
    try:
        command('stop-worker-2', COMPOSE + ['stop', 'spark-worker-2'])
        topology(1)
        bench('workers-1', 'benchmark_workers', ['--input', SMALL], 1)
    finally:
        command('restore-worker-2', COMPOSE + ['start', 'spark-worker-2'])
        topology(2)
    bench('workers-2', 'benchmark_workers', ['--input', SMALL], 2)
    inputs = [CONTAINER_DIR + '/' + name + '.json' for name in ['size', 'partitions', 'workers-1', 'workers-2']]
    command('minio-audit', COMPOSE + ['exec', '-T', 'spark-master', 'python3', '-m',
        'experiments.verify_benchmark_outputs'] + inputs + ['--report', CONTAINER_DIR + '/minio-audit.json',
        '--example-report', CONTAINER_DIR + '/quality-example.json'])
    for name in ['minio-audit', 'quality-example']:
        command('copy-' + name, [DOCKER, 'cp', 'spark-master:' + CONTAINER_DIR + '/' + name + '.json',
                                 str(DEST / (name + '.json'))])
    local_inputs = [str(DEST / (name + '.json')) for name in ['size', 'partitions', 'workers-1', 'workers-2']]
    command('plot', [sys.executable, '-m', 'experiments.plot_results'] + local_inputs +
            ['--csv', str(DEST / 'summary.csv'), '--runs-csv', str(DEST / 'runs.csv'),
             '--png', str(DEST / 'benchmark.png')])
    with (DEST / 'final-topology.json').open('x', encoding='utf-8') as out:
        json.dump(topology(2), out, indent=2)
    print('ALL_COMPLETE', flush=True)


if __name__ == '__main__':
    main()
