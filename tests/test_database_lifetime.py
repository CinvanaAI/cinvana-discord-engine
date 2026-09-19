import sqlite3
import pytest
from discord_engine.db import EngineDatabase


def test_initialize_closes_its_connection(tmp_path,monkeypatch):
    database=EngineDatabase(tmp_path/'archive.db');opened=[];connect=database.connect
    def tracked():
        connection=connect();opened.append(connection);return connection
    monkeypatch.setattr(database,'connect',tracked);database.initialize()
    assert len(opened)==1
    with pytest.raises(sqlite3.ProgrammingError):opened[0].execute('SELECT 1')
