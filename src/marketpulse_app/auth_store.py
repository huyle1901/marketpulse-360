"""PostgreSQL operational accounts, throttled login, sessions and access evidence."""
from __future__ import annotations
import hashlib
import secrets
import uuid
from importlib.resources import files
from .security import Principal, ROLES, password_hash, password_matches, username
from .store import VersionConflict

class AuthStore:
    def __init__(self,store):
        self.store=store
        self._dummy=password_hash(secrets.token_urlsafe(24))
    def initialize(self) -> None:
        with self.store.connect() as c:
            c.execute('SELECT pg_advisory_xact_lock(36009)')
            c.execute(files('marketpulse_app').joinpath('sql/009_security.sql').read_text())
    def create_user(self,name:str,password:str,role:str) -> Principal:
        name=username(name)
        if role not in ROLES:raise ValueError('invalid role')
        encoded=password_hash(password); identity=str(uuid.uuid4())
        with self.store.connect() as c:
            c.execute('INSERT INTO marketpulse_app.auth_users(user_id,username,password_hash,role) VALUES(%s,%s,%s,%s)',(identity,name,encoded,role))
        return Principal(identity,name,role)
    def set_password(self,name:str,password:str) -> None:
        name=username(name);encoded=password_hash(password)
        with self.store.connect() as c:
            c.execute('SELECT pg_advisory_xact_lock(360091)')
            row=c.execute('UPDATE marketpulse_app.auth_users SET password_hash=%s,version=version+1 WHERE username=%s RETURNING user_id',(encoded,name)).fetchone()
            if not row:raise ValueError('unknown account')
            c.execute('DELETE FROM marketpulse_app.auth_sessions WHERE user_id=%s',(row[0],))
    @staticmethod
    def digest(token:str) -> str:return hashlib.sha256(token.encode()).hexdigest()
    def login(self,name:str,password:str) -> tuple[str,Principal] | None:
        # Invalid names still receive identical work/error and bounded persisted throttling.
        valid_name=True
        try:name=username(name)
        except ValueError:name='invalid';valid_name=False
        key=self.digest(name)
        with self.store.connect() as c:
            c.execute('SELECT pg_advisory_xact_lock(360091)')
            attempts=c.execute("SELECT failures,window_start > now()-interval '15 minutes' FROM marketpulse_app.login_failures WHERE username_hash=%s",(key,)).fetchone()
            row=c.execute('SELECT user_id,username,role,password_hash,enabled FROM marketpulse_app.auth_users WHERE username=%s',(name,)).fetchone() if valid_name else None
            valid=password_matches(password,row[3] if row and row[4] else self._dummy)
            if attempts and attempts[1] and attempts[0]>=5:return None
            if not row or not row[4] or not valid:
                c.execute("INSERT INTO marketpulse_app.login_failures(username_hash,failures) VALUES(%s,1) ON CONFLICT(username_hash) DO UPDATE SET failures=CASE WHEN login_failures.window_start > now()-interval '15 minutes' THEN login_failures.failures+1 ELSE 1 END,window_start=CASE WHEN login_failures.window_start > now()-interval '15 minutes' THEN login_failures.window_start ELSE now() END",(key,))
                return None
            c.execute('DELETE FROM marketpulse_app.login_failures WHERE username_hash=%s',(key,))
            token=secrets.token_urlsafe(32)
            c.execute("INSERT INTO marketpulse_app.auth_sessions(token_hash,user_id,expires_at) VALUES(%s,%s,now()+interval '8 hours')",(self.digest(token),row[0]))
            return token,Principal(str(row[0]),row[1],row[2])
    def resolve(self,token:str) -> Principal | None:
        if not token or len(token)>128:return None
        with self.store.connect() as c:
            row=c.execute('SELECT u.user_id,u.username,u.role FROM marketpulse_app.auth_sessions s JOIN marketpulse_app.auth_users u USING(user_id) WHERE s.token_hash=%s AND s.expires_at>now() AND u.enabled',(self.digest(token),)).fetchone()
            return Principal(str(row[0]),row[1],row[2]) if row else None
    def logout(self,token:str) -> None:
        with self.store.connect() as c:c.execute('DELETE FROM marketpulse_app.auth_sessions WHERE token_hash=%s',(self.digest(token),))
    def users(self) -> list[dict]:
        with self.store.connect() as c:return [dict(zip(('user_id','username','role','enabled','version'),(str(r[0]),r[1],r[2],r[3],str(r[4])))) for r in c.execute('SELECT user_id,username,role,enabled,version FROM marketpulse_app.auth_users ORDER BY username LIMIT 100').fetchall()]
    def update_user(self,identity:str,role:str,enabled:bool,version:int) -> None:
        uuid.UUID(identity)
        if role not in ROLES or type(enabled) is not bool or type(version) is not int or version<1:raise ValueError('invalid account change')
        with self.store.connect() as c:
            c.execute('SELECT pg_advisory_xact_lock(360091)')
            row=c.execute('SELECT role,enabled,version FROM marketpulse_app.auth_users WHERE user_id=%s FOR UPDATE',(identity,)).fetchone()
            if not row or row[2]!=version:raise VersionConflict('stale account')
            if row[0]=='admin' and row[1] and (role!='admin' or not enabled):
                if c.execute("SELECT count(*) FROM marketpulse_app.auth_users WHERE enabled AND role='admin'").fetchone()[0]<=1:raise ValueError('last enabled administrator is protected')
            c.execute('UPDATE marketpulse_app.auth_users SET role=%s,enabled=%s,version=version+1 WHERE user_id=%s',(role,enabled,identity))
            c.execute('DELETE FROM marketpulse_app.auth_sessions WHERE user_id=%s',(identity,))
    def audit(self,request_id:str,user_id:str|None,role:str,route:str,decision:str,status:int|None) -> None:
        with self.store.connect() as c:c.execute('INSERT INTO marketpulse_app.access_audit(request_id,user_id,role,route,decision,status) VALUES(%s,%s,%s,%s,%s,%s)',(request_id,user_id,role,route,decision,status))
    def audit_events(self) -> list[dict]:
        with self.store.connect() as c:
            return [dict(zip(('event_id','observed_at','request_id','user_id','role','route','decision','status'),(str(r[0]),r[1].isoformat(),*r[2:]))) for r in c.execute('SELECT event_id,observed_at,request_id,user_id,role,route,decision,status FROM marketpulse_app.access_audit ORDER BY event_id DESC LIMIT 100').fetchall()]
    def expired_sessions(self) -> int:
        with self.store.connect() as c:return c.execute('SELECT count(*) FROM marketpulse_app.auth_sessions WHERE expires_at<=now()').fetchone()[0]
    def cleanup_sessions(self) -> int:
        with self.store.connect() as c:return len(c.execute('WITH expired AS (SELECT token_hash FROM marketpulse_app.auth_sessions WHERE expires_at<=now() ORDER BY expires_at LIMIT 1000 FOR UPDATE SKIP LOCKED) DELETE FROM marketpulse_app.auth_sessions WHERE token_hash IN (SELECT token_hash FROM expired) RETURNING 1').fetchall())
