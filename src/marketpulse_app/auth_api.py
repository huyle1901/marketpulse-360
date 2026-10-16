"""Cookie authentication HTTP boundary and explicit operator CLI provisioning."""
from dataclasses import asdict
from fastapi import HTTPException,Request,Response
from pydantic import BaseModel,ConfigDict,Field

class LoginInput(BaseModel):
    model_config=ConfigDict(extra='forbid')
    username:str=Field(max_length=64)
    password:str=Field(max_length=128)
class UserInput(BaseModel):
    model_config=ConfigDict(extra='forbid')
    role:str
    enabled:bool=Field(strict=True)
    expected_version:int=Field(strict=True,ge=1)

def register_auth_routes(app,access):
    @app.post('/api/v1/auth/login')
    def login(body:LoginInput,response:Response):
        result=access.auth.login(body.username,body.password)
        if result is None:raise HTTPException(401,'Invalid credentials or login temporarily blocked')
        token,principal=result
        response.set_cookie('mp_session',token,max_age=28800,httponly=True,samesite='strict',secure=access.secure_cookie,path='/')
        return asdict(principal)
    @app.get('/api/v1/auth/session')
    def session(request:Request):return asdict(request.state.principal)
    @app.post('/api/v1/auth/logout')
    def logout(request:Request,response:Response):
        access.auth.logout(request.cookies.get('mp_session',''))
        response.delete_cookie('mp_session',path='/',httponly=True,samesite='strict',secure=access.secure_cookie)
        return {'status':'signed_out'}
    @app.get('/api/v1/ops/users')
    def users():return access.auth.users()
    @app.put('/api/v1/ops/users/{user_id}')
    def update(user_id:str,body:UserInput):
        access.auth.update_user(user_id,body.role,body.enabled,body.expected_version)
        return {'status':'updated'}

def main():
    import argparse,os,sys
    from .auth_store import AuthStore
    from .settings import load_settings
    from .store import AppStore
    parser=argparse.ArgumentParser(description='Explicit application account provisioning; password on stdin')
    parser.add_argument('action',choices=('create','password'));parser.add_argument('username');parser.add_argument('--role',choices=('viewer','operator','admin'),default='viewer')
    args=parser.parse_args();auth=AuthStore(AppStore(load_settings(os.environ).app_dsn));auth.initialize()
    password=sys.stdin.readline().rstrip('\r\n')
    if args.action=='create':auth.create_user(args.username,password,args.role)
    else:auth.set_password(args.username,password)
    print('Account operation completed')
if __name__=='__main__':main()
