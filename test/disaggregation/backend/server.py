import os

from fastapi import FastAPI, Request
import uvicorn


app = FastAPI()


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.api_route("/", methods=["GET", "POST"])
@app.api_route("/v1/chat/completions", methods=["GET", "POST"])
async def echo(request: Request):
    return {
        "status": "ok",
        "role": os.environ.get("ROLE", ""),
        "revision": os.environ.get("REVISION", ""),
        "revision_header": request.headers.get("x-llm-d-disagg-revision"),
        "prefiller_host_port": request.headers.get("x-prefiller-host-port"),
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="warning")
