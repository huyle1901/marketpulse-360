"""Bounded authentication primitives and deny-by-default application capabilities."""
from __future__ import annotations
import hashlib
import hmac
import re
import secrets
from dataclasses import dataclass

ROLES = {'viewer': 0, 'operator': 1, 'admin': 2}

@dataclass(frozen=True, slots=True)
class Principal:
    user_id: str
    username: str
    role: str

def username(value: str) -> str:
    if not isinstance(value,str) or re.fullmatch(r'[a-zA-Z0-9_.-]{3,64}',value) is None:
        raise ValueError('invalid username')
    return value.lower()

def password_hash(value: str) -> str:
    if not isinstance(value,str) or not 12 <= len(value) <= 128: raise ValueError('invalid password length')
    salt=secrets.token_bytes(16)
    digest=hashlib.scrypt(value.encode(),salt=salt,n=32768,r=8,p=1,dklen=32,maxmem=64*1024*1024)
    return salt.hex()+':'+digest.hex()

def password_matches(value: str, encoded: str) -> bool:
    try:
        salt,digest=encoded.split(':'); raw=bytes.fromhex(salt); expected=bytes.fromhex(digest)
        if len(raw)!=16 or len(expected)!=32 or not isinstance(value,str) or len(value)>128:return False
        actual=hashlib.scrypt(value.encode(),salt=raw,n=32768,r=8,p=1,dklen=32,maxmem=64*1024*1024)
        return hmac.compare_digest(actual,expected)
    except (ValueError,TypeError):return False

# Patterns are server-owned templates; captured values are never telemetry labels.
ROUTES = (
 ('GET',r'dashboard|campaign-settings|alert-rules|alerts','viewer','aggregate'),
 ('GET',r'budget-pacing/[^/]+','viewer','budget-pacing/{campaign_id}'),
 ('POST',r'budget-simulation','viewer','budget-simulation'),
 ('GET',r'analysis/(funnel|attribution)','viewer','analysis/aggregate'),
 ('GET',r'analysis/journeys','operator','analysis/journeys'),
 ('GET',r'lifecycle/cohorts','viewer','lifecycle/cohorts'),
 ('GET',r'lifecycle/(?:email-engagement|customer-mix)','viewer','lifecycle/aggregate'),
 ('GET',r'insights/(?:activity|order-values|loyalty|channels)','viewer','insights/aggregate'),
 ('GET',r'lifecycle/profiles','operator','lifecycle/profiles'),
 ('GET',r'lifecycle/profiles/[^/]+/timeline','operator','lifecycle/profiles/{profile_ref}/timeline'),
 ('GET',r'lifecycle/segments(?:/[^/]+/versions/[0-9]+)?','operator','lifecycle/segments'),
 ('POST',r'lifecycle/segments(?:/preview)?','operator','lifecycle/segments'),
 ('PUT',r'lifecycle/segments/[^/]+','operator','lifecycle/segments/{segment_id}'),
 ('PUT',r'(campaign-settings|alert-rules)/[^/]+','operator','configuration/{id}'),
 ('POST',r'alert-rules|alerts/evaluate','operator','configuration'),
 ('GET',r'auth/session','viewer','auth/session'),
 ('POST',r'auth/logout','viewer','auth/logout'),
 ('GET',r'ops/(status|quality|lineage|audit|users|retention)','admin','ops/read'),
 ('POST',r'ops/(quality/check|quality/probe|quality/probe/clear|retention/sessions)','admin','ops/action'),
 ('PUT',r'ops/users/[^/]+','admin','ops/users/{user_id}'),
)
def policy(method: str,path: str) -> tuple[str,str] | None:
    if method=='GET' and path in ('/docs','/redoc','/openapi.json'):return ('docs','admin')
    if not path.startswith('/api/v1/'):return None
    tail=path[8:]
    for verb,pattern,role,template in ROUTES:
        if method==verb and re.fullmatch(pattern,tail):return ('/api/v1/'+template,role)
    return None
