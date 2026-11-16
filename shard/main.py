import os
import asyncio
from typing import Any, Optional
from contextlib import asynccontextmanager
import httpx
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel

SHARD_ID = os.getenv("SHARD_ID", "shard-1")
SHARD_URL = os.getenv("SHARD_URL", "http://localhost:8001")
COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://localhost:8000")

# Сховище в пам'яті: {table_name: {composite_key: value}}
storage: dict[str, dict[str, Any]] = {}


def make_composite_key(partition_key: str, sort_key: Optional[str] = None) -> str:
    return f"{partition_key}#{sort_key}" if sort_key else partition_key


async def register_with_coordinator():
    """Реєструє поточний шард у координаторі при запуску (з повторними спробами)."""
    payload = {"shard_id": SHARD_ID, "url": SHARD_URL}
    for attempt in range(1, 11):
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(f"{COORDINATOR_URL}/shards/register", json=payload, timeout=3.0)
                if resp.status_code == 200:
                    print(f"[{SHARD_ID}] Успішно зареєстровано в координаторі ({COORDINATOR_URL})")
                    return
        except Exception as e:
            print(f"[{SHARD_ID}] Спроба {attempt}/10 реєстрації не вдалася: {e}")
        await asyncio.sleep(2)


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(register_with_coordinator())
    yield


app = FastAPI(title=f"KV Store Shard ({SHARD_ID})", lifespan=lifespan)


class ItemPayload(BaseModel):
    partition_key: str
    sort_key: Optional[str] = None
    value: Any


@app.post("/internal/{table_name}/items", status_code=201)
def create_item(table_name: str, item: ItemPayload):
    table = storage.setdefault(table_name, {})
    comp_key = make_composite_key(item.partition_key, item.sort_key)
    table[comp_key] = item.value
    return {
        "shard_id": SHARD_ID,
        "partition_key": item.partition_key,
        "sort_key": item.sort_key,
        "value": item.value,
    }


@app.head("/internal/{table_name}/items/{partition_key}")
def exists_item(table_name: str, partition_key: str, sort_key: Optional[str] = None):
    comp_key = make_composite_key(partition_key, sort_key)
    if table_name not in storage or comp_key not in storage[table_name]:
        raise HTTPException(status_code=404, detail="Item not found")
    return Response(status_code=200)


@app.get("/internal/{table_name}/items/{partition_key}")
def read_item(table_name: str, partition_key: str, sort_key: Optional[str] = None):
    comp_key = make_composite_key(partition_key, sort_key)
    if table_name not in storage or comp_key not in storage[table_name]:
        raise HTTPException(status_code=404, detail="Item not found")
    return {
        "shard_id": SHARD_ID,
        "partition_key": partition_key,
        "sort_key": sort_key,
        "value": storage[table_name][comp_key],
    }


@app.delete("/internal/{table_name}/items/{partition_key}", status_code=204)
def delete_item(table_name: str, partition_key: str, sort_key: Optional[str] = None):
    comp_key = make_composite_key(partition_key, sort_key)
    if table_name not in storage or comp_key not in storage[table_name]:
        raise HTTPException(status_code=404, detail="Item not found")
    del storage[table_name][comp_key]
    return Response(status_code=204)