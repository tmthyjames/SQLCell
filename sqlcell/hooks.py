from sqlalchemy import create_engine    
from sqlcell.db import DBSessionHandler

class HookHandler(DBSessionHandler):
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
                key_engine, cmd = [i.strip() for i in hook.split('=', 1)]
                key, engine = key_engine.split(' ')
                engine = engine
                cmds_to_add.append((key, engine, cmd))

        for key, engine, cmd in cmds_to_add:
            engine = self.db_info.get(engine, engine)
            is_engine = self.is_engine(engine)
            if not is_engine:
                raise Exception('Alias not found or engine argument error')
            self.session.add(self.Hooks(key=key, engine=engine, cmd=cmd))
        self.session.commit()
        return self
        
    def run(self, cell, engine_var):
        cell = cell.replace('~', '').split(' ')
        cell, cmd_args = cell[0], cell[1:]
        hook_query = self.session.query(self.Hooks).filter_by(key=cell).first()
        hook_cmd = hook_query.cmd
        if engine_var:
            hook_engine = create_engine(str(engine.url))
        else:
            hook_engine = create_engine(hook_query.engine)
        self.hook_engine = hook_engine
        return hook_engine, hook_cmd.format(*cmd_args)

    def list(self, *srgs, **kwargs):
        for row in self.session.query(self.Hooks).all():
            print(row.key, "|", row.cmd, " | Engine: ", row.engine)
    
    def refresh(self, cell):
        self.session.query(Hooks).delete()
        self.session.commit()
