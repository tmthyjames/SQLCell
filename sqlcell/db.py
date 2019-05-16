from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import desc, asc
from sqlalchemy.engine.base import Engine

class DBSessionHandler(object):
    def __init__(self):
        Base = automap_base()
        engine = create_engine("sqlite:///sqlcell.db")
        Base.prepare(engine, reflect=True)
        self.classes = Base.classes
        self.tables = Base.metadata.tables.keys()
        self.Sqlcell = Base.classes.sqlcell
        self.Engines = Base.classes.engines
        self.Hooks = Base.classes.hooks
        Session = sessionmaker(autoflush=False)
        Session.configure(bind=engine)
        self.session = Session()

        dbs = self.session.query(self.Engines).all()
        self.db_info = {}
        for row in dbs:
            engine = row.engine
            self.db_info[row.db] = engine
            self.db_info[engine] = engine
            self.db_info[row.host] = engine
            
    def recycle(self):
        pass
    
    def create(self):
        pass
    
    def dispose(self):
        pass

class EngineHandler(DBSessionHandler):
    """remove all engines from sqlcell.db:
       %%sql refresh
       may have to use @cell_line_magic.
       add multiple new engines:
       %%sql add
       foo=<engine str>
       bar=<another engine str>
       baz=<another engine str>"""
    def __init__(self, *args, **kwargs):
        super().__init__()
        
    def view(self):
        "show all alias/engines"
        pass
    
    def refresh(self, cell):
        self.session.query(self.Engines).delete()
        self.session.commit()

    def alias(self):
        "allow alias for each engine"
        pass

    def list(self, *srgs, **kwargs):
        pass

