"""Worker HTTP (FastAPI). Lê o CSV de DATA_PATH; só recebe intervalos de bytes."""
import os

from fastapi import FastAPI
from pydantic import BaseModel

from common import process_block

app = FastAPI()


class Block(BaseModel):
    block_id: int
    start: int
    end: int


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/process")
def process(b: Block):
    path = os.environ.get("DATA_PATH", "data/sample.csv")
    return {"block_id": b.block_id, "agregado": process_block(path, b.start, b.end)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), log_level="warning")
