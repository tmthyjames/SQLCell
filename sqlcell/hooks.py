from sqlalchemy import create_engine
from sqlcell.db import DBSessionHandler, EngineHandler
import pandas as pd

class HookHandler(EngineHandler):
    """input common queries to remember with a key/value pair. ie,
       %%sql hook
       \d=<common query>"
       \dt=<another common query>"""
    def __init__(self, engine, *args, **kwargs):
        super().__init__()
        self.hook_engine = engine

    def is_engine(self, engine: str):
        try:
            create_engine(engine)
            return True
        except:
            return False

    def add(self, line, cell):
        "add hook to db"
        cmds_to_add = []
        hooks = cell.split('\n\n')
        for hook in hooks:
            hook = hook.strip()
            if hook:
                key, cmd = [i.strip() for i in hook.split('=', 1)]
                cmds_to_add.append((key, cmd))

        for key, cmd in cmds_to_add:
            self.session.add(self.Hooks(key=key, engine='', cmd=cmd))
        self.session.commit()
        return self

    def run(self, cell, engine_var):
        cell = cell.replace('~', '').split(' ')
        engine_alias, sql, cmd_args = cell[0], cell[1], cell[2:]
        hook_query = self.session.query(self.Hooks).filter_by(key=sql).first()
        hook_cmd = hook_query.cmd
        hook_engine = self.get_engine(engine_alias)
        self.hook_engine = hook_engine
        return hook_engine, hook_cmd.format(*cmd_args)

    def list(self, *srgs, **kwargs):
        hooks = []
        for row in self.session.query(self.Hooks).all():
            hook = {
                'Alias': row.key,
                'Hook': row.cmd,
                'Engine': row.engine
            }
            hooks.append(hook)
        return pd.DataFrame(hooks)

    def refresh(self, cell):
        self.session.query(self.Hooks).delete()
        self.session.commit()
