import logging

from fastapi import FastAPI, Request
from starlette.middleware.base import BaseHTTPMiddleware

from .bmw_router import bmw_router
from .kia_router import kia_router
from .volksvagen_router import volkswagen_router

app = FastAPI(
    title="Vehicle Data Receiver",
    version="1.0.0",
    openapi_prefix="/to_be_defined_by_customer",
)
logging.basicConfig(level=logging.INFO)


# Middleware for testing
class LogRequestMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        logging.info(f"Request Headers: {dict(request.headers)}")
        response = await call_next(request)
        return response


# Add middleware to FastAPI app
app.add_middleware(LogRequestMiddleware)

app.include_router(volkswagen_router)
app.include_router(bmw_router)
app.include_router(kia_router)

