import hashlib
import bisect
from typing import Any, Optional
import httpx
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel

app = FastAPI(title="Distributed KV Store Coordinator", version="2.0.0")


class ConsistentHashRing:
    def __init__(self, virtual_nodes: int = 100):
        self.virtual_nodes = virtual_nodes
        self.ring: dict[int, str] = {}
        self.sorted_keys: list[int] = []
        self.shards: dict[str, str] = {}  # {shard_id: url}

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)

    def add_shard(self, shard_id: str, url: str):
        if shard_id in self.shards:
            self.shards[shard_id] = url
            return
        self.shards[shard_id] = url
        for i in range(self.virtual_nodes):
            vnode_key = f"{shard_id}#vnode-{i}"
            h = self._hash(vnode_key)
            self.ring[h] = shard_id
            bisect.insort(self.sorted_keys, h)

    def get_shard(self, partition_key: str) -> tuple[str, str]:
        if not self.sorted_keys:
            raise HTTPException(status_code=503, detail="No shards registered in cluster")
        h = self._hash(partition_key)
        idx = bisect.bisect(self.sorted_keys, h)
        if idx == len(self.sorted_keys):
            idx = 0
        shard_id = self.ring[self.sorted_keys[idx]]
        return shard_id, self.shards[shard_id]


hash_ring = ConsistentHashRing(virtual_nodes=100)
tables: dict[str, dict] = {}


class ShardRegistration(BaseModel):
    shard_id: str
    url: str


class TableDefinition(BaseModel):
    table_name: str
    partition_key: str
    sort_key: Optional[str] = None
    attributes: Optional[dict[str, str]] = None


class ItemCreate(BaseModel):
    partition_key: str
    sort_key: Optional[str] = None
    value: Any


@app.post("/shards/register")
def register_shard(data: ShardRegistration):
    hash_ring.add_shard(data.shard_id, data.url)
    print(f"[Coordinator] Зареєстровано шард: {data.shard_id} -> {data.url}")
    return {"status": "registered", "active_shards": list(hash_ring.shards.keys())}


@app.get("/shards")
def list_shards():
    return {"shards": hash_ring.shards, "virtual_nodes_per_shard": hash_ring.virtual_nodes}


@app.post("/tables", status_code=201)
def register_table(table: TableDefinition):
    if table.table_name in tables:
        raise HTTPException(status_code=409, detail="Table already exists")
    tables[table.table_name] = table.model_dump()
    return tables[table.table_name]


def validate_table_and_keys(table_name: str, sort_key: Optional[str]):
    if table_name not in tables:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    table_def = tables[table_name]
    if table_def.get("sort_key") and not sort_key:
        raise HTTPException(
            status_code=400,
            detail=f"Table '{table_name}' requires sort_key '{table_def['sort_key']}'",
        )


@app.post("/tables/{table_name}/items", status_code=201)
async def create_item(table_name: str, item: ItemCreate):
    validate_table_and_keys(table_name, item.sort_key)
    shard_id, shard_url = hash_ring.get_shard(item.partition_key)

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{shard_url}/internal/{table_name}/items",
            json=item.model_dump(),
            timeout=5.0,
        )
    if resp.status_code != 201:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return resp.json()


@app.head("/tables/{table_name}/items/{partition_key}")
async def exists_item(table_name: str, partition_key: str, sort_key: Optional[str] = None):
    validate_table_and_keys(table_name, sort_key)
    _, shard_url = hash_ring.get_shard(partition_key)
    params = {"sort_key": sort_key} if sort_key else {}

    async with httpx.AsyncClient() as client:
        resp = await client.head(
            f"{shard_url}/internal/{table_name}/items/{partition_key}",
            params=params,
            timeout=5.0,
        )
    if resp.status_code == 404:
        raise HTTPException(status_code=404, detail="Item not found")
    return Response(status_code=200)


@app.get("/tables/{table_name}/items/{partition_key}")
async def read_item(table_name: str, partition_key: str, sort_key: Optional[str] = None):
    validate_table_and_keys(table_name, sort_key)
    _, shard_url = hash_ring.get_shard(partition_key)
    params = {"sort_key": sort_key} if sort_key else {}

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{shard_url}/internal/{table_name}/items/{partition_key}",
            params=params,
            timeout=5.0,
        )
    if resp.status_code == 404:
        raise HTTPException(status_code=404, detail="Item not found")
    return resp.json()


@app.delete("/tables/{table_name}/items/{partition_key}", status_code=204)
async def delete_item(table_name: str, partition_key: str, sort_key: Optional[str] = None):
    validate_table_and_keys(table_name, sort_key)
    _, shard_url = hash_ring.get_shard(partition_key)
    params = {"sort_key": sort_key} if sort_key else {}

    async with httpx.AsyncClient() as client:
        resp = await client.delete(
            f"{shard_url}/internal/{table_name}/items/{partition_key}",
            params=params,
            timeout=5.0,
        )
    if resp.status_code == 404:
        raise HTTPException(status_code=404, detail="Item not found")
    return Response(status_code=204)