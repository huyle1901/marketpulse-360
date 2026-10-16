"""Explicit idempotent local secret creation; never prints secret contents."""
from pathlib import Path
import os
import secrets

def setup(root:Path) -> None:
    if root.is_symlink():raise ValueError('secret directory must not be a symlink')
    root.mkdir(mode=0o700,parents=True,exist_ok=True);root.chmod(0o700)
    service_names=('metrics_token','grafana_password')
    account_names=tuple(f'{role}_password' for role in ('admin','operator','viewer'))
    names=service_names+account_names
    # Preflight before creating any leaf.
    for name in names:
        target=root/name
        if target.is_symlink() or target.exists() and not target.is_file():raise ValueError('secret target must be a regular file')
    for name in names:
        # These are filesystem permission bits, never credential values. Keep
        # them separate from filenames to avoid password-assignment ambiguity.
        mode=0o444 if name in service_names else 0o600
        target=root/name
        try:
            fd=os.open(target,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,mode)
        except FileExistsError:continue
        with os.fdopen(fd,'w') as stream:stream.write(secrets.token_urlsafe(32)+'\n')
        target.chmod(mode)

def main():
    import argparse
    p=argparse.ArgumentParser();p.add_argument('--directory',type=Path,default=Path('.secrets'));args=p.parse_args();setup(args.directory)
    print('Missing local secrets prepared; existing values preserved')
if __name__=='__main__':main()
