from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from payment_service.exceptions import AppError


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(AppError)
    async def app_error_handler(_request: Request, exc: AppError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={"code": exc.code, "message": exc.message},
        )

    @app.exception_handler(RequestValidationError)
    async def validation_handler(_request: Request, exc: RequestValidationError) -> JSONResponse:
        details = []
        for error in exc.errors():
            details.append(
                {
                    "type": error.get("type"),
                    "loc": [str(item) for item in error.get("loc", ())],
                    "msg": error.get("msg"),
                }
            )
        return JSONResponse(
            status_code=422,
            content={"code": "validation_error", "message": "Request validation failed", "details": details},
        )
