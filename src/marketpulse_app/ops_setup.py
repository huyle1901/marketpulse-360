"""Explicit idempotent local secret creation; never prints secret contents."""
from pathlib import Path
import os
import secrets

def setup(root:Path) -> None:
    if root.is_symlink():raise ValueError('secret directory must not be a symlink')
    root.mkdir(mode=0o700,parents=True,exist_ok=True);root.chmod(0o700)
    names={'metrics_token':0o444,'grafana_password':0o444,'admin_password':0o600,'operator_password':0o600,'viewer_password':0o600}
    # Preflight before creating any leaf.
    for name in names:
        target=root/name
        if target.is_symlink() or target.exists() and not target.is_file():raise ValueError('secret target must be a regular file')
    for name,mode in names.items():
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
