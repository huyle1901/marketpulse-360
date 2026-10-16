"""Explicit credential-file proof transport; no anonymous fallback or token output."""
from __future__ import annotations
import http.cookiejar
import json
import urllib.request
from pathlib import Path
from urllib.parse import urlsplit
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):return None
class ProofHTTP:
    def __init__(self,api:str,origin:str,username:str,password_file:Path,web:str|None=None):
        self.api=api.rstrip('/');self.origin=origin
        self.allowed={(urlsplit(u).scheme,urlsplit(u).netloc) for u in (api,web) if u}
        self.opener=urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()),NoRedirect())
        req=urllib.request.Request(self.api+'/api/v1/auth/login',data=json.dumps({'username':username,'password':password_file.read_text().strip()}).encode(),headers={'Content-Type':'application/json','Origin':origin})
        with self.opener.open(req,timeout=15) as response:response.read()
    def open(self,request,timeout=30):
        if isinstance(request,str):request=urllib.request.Request(request)
        parts=urlsplit(request.full_url)
        if (parts.scheme,parts.netloc) not in self.allowed or parts.username or parts.password:raise ValueError('proof request outside selected app origins')
        if request.get_method() in ('POST','PUT','DELETE','PATCH'):request.add_header('Origin',self.origin)
        return self.opener.open(request,timeout=timeout)

def add_auth_arguments(parser):
    parser.add_argument('--username',required=True)
    parser.add_argument('--password-file',required=True,type=Path)
    parser.add_argument('--origin',default='http://localhost:43100')
