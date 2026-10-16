"""HTTP authorization without trusting browser role claims or logging user input."""
from __future__ import annotations
import hmac
import uuid
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool
from .security import ROLES,policy

class AccessControl:
    def __init__(self,auth,origin:str,secure_cookie:bool=False,metrics_token:str=''):
        self.auth=auth; self.origin=origin; self.secure_cookie=secure_cookie; self.metrics_token=metrics_token
        self.audit_outcome_failures=0
    async def enforce(self,request,call_next):
        path=request.url.path;method=request.method
        if path in ('/api/v1/health/live','/api/v1/health/ready') and method=='GET':return await call_next(request)
        if path=='/metrics':
            token=request.headers.get('authorization','')
            if not self.metrics_token or not hmac.compare_digest(token,'Bearer '+self.metrics_token):return JSONResponse({'code':'unauthorized'},status_code=401)
            request.state.route_template='/metrics'
            return await call_next(request)
        target=policy(method,path); template=target[0] if target else 'unknown'
        login=path=='/api/v1/auth/login' and method=='POST'
        if login:template='/api/v1/auth/login'
        request.state.route_template=template; request_id=str(uuid.uuid4())
        principal=await run_in_threadpool(self.auth.resolve,request.cookies.get('mp_session',''))
        status=None
        if method in ('POST','PUT','DELETE','PATCH') and request.headers.get('origin')!=self.origin:status=403
        elif not login and principal is None:status=401
        elif not login and (target is None or ROLES.get(principal.role,-1)<ROLES[target[1]]):status=403
        role=principal.role if principal else 'anonymous';identity=principal.user_id if principal else None
        await run_in_threadpool(self.auth.audit,request_id,identity,role,template,'deny' if status else 'allow',status)
        if status:return JSONResponse({'code':'unauthorized' if status==401 else 'forbidden'},status_code=status)
        request.state.principal=principal
        response=await call_next(request)
        try:await run_in_threadpool(self.auth.audit,request_id,identity,role,template,'outcome',response.status_code)
        except Exception:self.audit_outcome_failures+=1
        response.headers['X-Request-ID']=request_id
        return response
