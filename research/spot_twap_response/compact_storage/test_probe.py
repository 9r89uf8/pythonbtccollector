"""The experiment must reject unsafe destinations before touching schemas."""
import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from research.spot_twap_response.compact_storage import probe


@pytest.mark.parametrize('name', ['price_collector', 'postgres', '', None,
    'ghost_compact_storage_validation_', 'ghost_compact_storage_validation_x;drop',
    'ghost_compact_storage_validation_../production', 'ghost_compact_storage_validation_'+'x'*36])
def test_database_guard_refuses_production_and_bad_names(name):
    with pytest.raises(ValueError, match='Refusing database'):
        probe.check_database_name(name)


def test_database_guard_allows_only_named_disposable_database():
    probe.check_database_name('ghost_compact_storage_validation_ab123456')


@pytest.mark.parametrize('size,free,message', [
    (probe.MAX_DB_BYTES-probe.WRITE_RESERVE_BYTES,probe.MIN_FREE_BYTES,'allocation'),
    (100,probe.MIN_FREE_BYTES-1,'Filesystem'),
])
def test_size_and_disk_guard_include_pending_reserve(monkeypatch,size,free,message):
    class Connection:
        async def fetchval(self,sql):
            assert 'pg_database_size' in sql
            return size
    monkeypatch.setattr(probe.shutil,'disk_usage',lambda _:SimpleNamespace(free=free))
    experiment=probe.Experiment(Connection(),None,{}, {})
    experiment.directory=Path('.')
    with pytest.raises(ValueError,match=message):
        asyncio.run(experiment.guard())


def test_timeout_guard_precedes_another_database_statement(monkeypatch):
    class Connection:
        async def fetchval(self,_):
            pytest.fail('Expired experiment must not issue another query')
    experiment=probe.Experiment(Connection(),None,{}, {})
    monkeypatch.setattr(probe.time,'monotonic_ns',lambda:experiment.started+probe.MAX_SECONDS*1000000000)
    with pytest.raises(ValueError,match='time cap'):
        asyncio.run(experiment.guard())


def test_guard_is_read_only_on_production_connection(monkeypatch):
    calls=[]
    class Connection:
        async def fetchval(self,sql):
            calls.append(sql)
            assert sql == 'SELECT current_database()'
            return 'price_collector'
        async def execute(self,*_):
            pytest.fail('No SQL write or session change before database name guard')
        async def close(self,timeout):
            calls.append('close')
    import asyncpg
    async def connect(*_,**__):
        return Connection()
    monkeypatch.setattr(asyncpg,'connect',connect)
    monkeypatch.setattr(probe,'inspect_input',lambda *_:({},0,0))
    with pytest.raises(ValueError,match='Refusing database'):
        asyncio.run(probe.run('irrelevant',None,{},{}))
    assert calls == ['SELECT current_database()','close']
