"""Project two synthetic conversation chunks into a real paused archive plan."""
from __future__ import annotations
import hashlib,json,tempfile
from datetime import date
from pathlib import Path
from discord_engine.config import EngineConfig
from discord_engine.db import EngineDatabase
from discord_engine.repository import Repository
from discord_engine.ce_client import CEChunk
from discord_engine.projection import project_chunks


def demonstrate(root):
    config=EngineConfig.load(root);config.create_runtime();db=EngineDatabase(config.state_db);db.initialize();connection=db.connect();repo=Repository(connection)
    repo.configure_bot_head(bot_head_key='demo',display_name='Synthetic Archive',token_env='SYNTHETIC_ARCHIVE_TOKEN')
    repo.configure_guild(guild_key='demo',name='Synthetic Archive',purpose='Offline projection',server_model='chronological_private',bot_head_key='demo',discord_guild_id=None)
    chunks=[]
    for n,text in enumerate(['First, review the fixture.','Then, record the result.'],1):
        digest=hashlib.sha256(text.encode()).hexdigest();day=date(2026,1,n)
        chunks.append(CEChunk(message_id=f'message-{n}',result_id=f'result-{n}',audience='private',chunk_index=1,start=0,end=len(text),max_chars=2000,source={'kind':'source_revision','id':f'revision-{n}','sha256':digest},content=text,content_sha256=digest,chat_id='synthetic-workshop',position=n,speaker='User',message_timestamp=f'{day.isoformat()}T12:00:00+00:00',day=day))
    report=project_chunks(repo,guild_key='demo',source_system='synthetic-ce',start=date(2026,1,1),end=date(2026,1,2),chunks=chunks);connection.commit()
    result={'mode':'Synthetic CE chunks; actual archive projection','source_messages':[x.content for x in chunks],'projection':report.to_dict(),'publication_paused':repo.is_paused('publication'),'remaining_actions':repo.remaining_actions('demo'),'sample':repo.calendar_sample('demo',5),'discord_calls':0}
    connection.close();return result


if __name__=='__main__':
    with tempfile.TemporaryDirectory(prefix='archive-demo-') as scratch:print(json.dumps(demonstrate(Path(scratch)/'archive'),indent=2))
