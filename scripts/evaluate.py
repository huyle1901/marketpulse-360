#!/usr/bin/env python3
"""MP-10 isolated benchmark/evidence CLI. See docs/runbooks/evaluation.md."""
from __future__ import annotations
import argparse
import json
import os
import secrets
from pathlib import Path
from marketpulse_eval.runner import ROOT, dc, run_stream
from marketpulse_eval.queries import run_analytics
from marketpulse_eval.workload import STREAM_SCALES
from marketpulse_eval.analytics import ANALYTICS_SCALES


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',required=True,type=Path)
    commands = parser.add_subparsers(dest='command',required=True)
    commands.add_parser('setup')
    stream = commands.add_parser('stream'); stream.add_argument('--engine',choices=('flink','streams'),required=True)
    stream.add_argument('--scale',choices=STREAM_SCALES,default='eval-small');stream.add_argument('--recovery',action='store_true')
    stream.add_argument('--eps',type=int,default=0)
    stream.add_argument('--warmup',action='store_true',help='label separate untabulated warmup run')
    analytics = commands.add_parser('analytics');analytics.add_argument('--engine',choices=('clickhouse','postgres'),required=True)
    analytics.add_argument('--scale',choices=ANALYTICS_SCALES,default='analytics-1m')
    analytics.add_argument('--warmups',type=int,default=20);analytics.add_argument('--samples',type=int,default=100)
    app=commands.add_parser('app')
    app.add_argument('--api',default='http://localhost:48000');app.add_argument('--origin',default='http://localhost:43100')
    app.add_argument('--username',required=True);app.add_argument('--password-file',required=True,type=Path)
    package=commands.add_parser('package');package.add_argument('--evidence',required=True,type=Path)
    package.add_argument('--group-size',type=int)
    args = parser.parse_args(); output = args.output.resolve()
    if not any(output.is_relative_to(ROOT / folder) for folder in ('.lakehouse','data/generated')):
        parser.error('output must be under ignored .lakehouse/ or data/generated/')
    output.parent.mkdir(parents=True,exist_ok=True)
    if args.command == 'setup':
        root = ROOT / '.secrets'; root.mkdir(mode=0o700,exist_ok=True)
        path = root / 'evaluation_password'
        if not path.exists():
            fd = os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
            with os.fdopen(fd,'w') as handle: handle.write(secrets.token_urlsafe(32)+'\n')
        if not path.read_text().strip():raise ValueError('empty evaluation credential file')
        result = {'version':1,'kind':'setup','credentials':'existing preserved or newly generated; never emitted'}
    elif args.command == 'stream':
        result = run_stream(args.engine,args.scale,recovery=args.recovery,eps=args.eps)
        result['warmup']=args.warmup
    elif args.command == 'analytics':
        result = run_analytics(args.engine,args.scale,warmups=args.warmups,samples=args.samples)
    elif args.command == 'app':
        from marketpulse_eval.effectiveness import evaluate_app
        result=evaluate_app(args.api,args.origin,args.username,args.password_file)
    else:
        from marketpulse_eval.package import build_package
        paths=[p for p in sorted(args.evidence.glob('*.json')) if json.loads(p.read_text()).get('kind') in ('stream','analytics','app')]
        index=build_package(paths,output,args.group_size)
        print(json.dumps({'output':str(output),'technical_evidence':index['technical_evidence'],'acceptance':index['acceptance']}))
        return
    output.write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
    print(json.dumps({'output':str(output),'kind':result['kind'],'engine':result.get('engine'),'scale':result.get('scale'),'passed':True}))


if __name__ == '__main__': main()
