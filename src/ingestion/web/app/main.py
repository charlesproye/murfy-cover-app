import logging
from fastapi import FastAPI, Request
from starlette.middleware.base import BaseHTTPMiddleware

from .volksvagen_router import volkswagen_router
from .bmw_router import bmw_router

app = FastAPI(
    title="Vehicle Data Receiver",
    version="1.0.0",
    openapi_prefix="/to_be_defined_by_customer",
)
logging.basicConfig(level=logging.INFO)


# Middleware for testing
class LogRequestMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # # Read request body (must buffer and reset)
        # body = await request.body()

        # Log headers
        logging.info(f"Request Headers: {dict(request.headers)}")

        # # Log body
        # logging.info(f"Request Body: {body.decode('utf-8')}")

        # # Recreate the request stream for downstream (body can be read only once)
        # request._receive = lambda: {"type": "http.request", "body": body}

        response = await call_next(request)
        return response


# Add middleware to FastAPI app
app.add_middleware(LogRequestMiddleware)

app.include_router(volkswagen_router)
app.include_router(bmw_router)

