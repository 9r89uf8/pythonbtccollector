import asyncio
from types import SimpleNamespace

import pytest

import price_collector.db as db


def test_create_read_pool_prefers_read_database_url(monkeypatch):
    calls = []

    async def fake_create_pool(database_url):
        calls.append(database_url)
        return "pool"

    settings = SimpleNamespace(
        DATABASE_URL="postgresql://writer@127.0.0.1:5432/price_collector",
        READ_DATABASE_URL="postgresql://reader@127.0.0.1:5432/price_collector",
    )
    monkeypatch.setattr(db, "create_pool", fake_create_pool)

    result = asyncio.run(db.create_read_pool(settings))

    assert result == "pool"
    assert calls == ["postgresql://reader@127.0.0.1:5432/price_collector"]


def test_create_read_pool_falls_back_to_database_url(monkeypatch):
    calls = []

    async def fake_create_pool(database_url):
        calls.append(database_url)
        return "pool"

    settings = SimpleNamespace(
        DATABASE_URL="postgresql://writer@127.0.0.1:5432/price_collector",
        READ_DATABASE_URL=None,
    )
    monkeypatch.setattr(db, "create_pool", fake_create_pool)

    result = asyncio.run(db.create_read_pool(settings))

    assert result == "pool"
    assert calls == ["postgresql://writer@127.0.0.1:5432/price_collector"]


def test_create_read_pool_requires_at_least_one_database_url(monkeypatch):
    async def fake_create_pool(database_url):
        raise AssertionError("create_pool should not be called without a database URL")

    settings = SimpleNamespace(DATABASE_URL=None, READ_DATABASE_URL=None)
    monkeypatch.setattr(db, "create_pool", fake_create_pool)

    with pytest.raises(
        RuntimeError,
        match="READ_DATABASE_URL or DATABASE_URL must be set for the API",
    ):
        asyncio.run(db.create_read_pool(settings))
