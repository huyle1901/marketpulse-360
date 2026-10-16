class TrustedTestAccess:
    async def enforce(self,request,call_next):
        return await call_next(request)
