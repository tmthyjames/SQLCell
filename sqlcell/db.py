from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import desc, asc
from sqlalchemy.engine.base import Engine
import re

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
            if row.db:
                self.db_info[row.db] = engine
            if row.alias:
                self.db_info[row.alias] = engine
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
        super(EngineHandler, self).__init__()
        
    def list(self):
        "show all alias/engines"
        for row in self.session.query(self.Engines).filter(self.Engines.alias != None):
            print(row.alias, " | ", row.engine)

    @property
    def latest_engine(self) -> Engine:
        record = self.session.query(self.Engines).order_by(desc(self.Engines.dt)).limit(1).first()
        if record:
            engine = record.engine
            return create_engine(engine)
    
    def add_engine(self, engine: Engine, alias: str=None) -> None:
        if isinstance(engine, str):
            engine = make_url(engine)
        else:
            engine = engine.url
        host = engine.host
        db = engine.database
        engine_str = str(engine)
        engine_exists_check = self.session.query(self.Engines).filter_by(db=db, host=host, engine=engine_str).first()
        if engine_exists_check: return None
        self.session.add(self.Engines(db=db, host=host, engine=engine_str, alias=alias))
        self.session.commit()

    def add_alias(self, cell):
        for i in re.split('\n{1,}', cell):
            row = i.replace(' ', '').split('=', 1)
            if row[1:]:
                print(row[:])
                alias, engine = row
                self.add_engine(engine, alias=alias)
        return ('Engines successfully registered')
    
    def refresh(self, cell):
        self.session.query(self.Engines).delete()
        self.session.commit()

