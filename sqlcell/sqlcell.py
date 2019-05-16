from IPython.core.magic import (Magics, magics_class, line_magic,
                                cell_magic, line_cell_magic)
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import desc, asc
from sqlalchemy.engine.base import Engine
from sqlalchemy import sql
import pandas as pd
import pickle
################# SQLCell modules #################
from sqlcell.db import EngineHandler, DBSessionHandler
from sqlcell.args import ArgHandler
from sqlcell.hooks import HookHandler
from sqlcell._initdb import run


@magics_class
class SQLCell(Magics, DBSessionHandler):
    
    current_engine = False
    current_hook_engine = False
    modes = ['query', 'hook', 'refresh']
    # consider yaml file for these types of params:
    hook_indicator = '~'
    
    def __init__(self, shell, data):
        # You must call the parent constructor
        super().__init__(shell)
        self.shell = shell
        self.data = data
        self.ipy = get_ipython()
        self.refresh_options = ['hooks', 'engines']

    @property
    def latest_engine(self) -> Engine:
        record = self.session.query(self.Engines).order_by(desc(self.Engines.dt)).limit(1).first()
        if record:
            engine = record.engine
            return create_engine(engine)
    
    def add_engine(self, engine: Engine) -> None:
        host = engine.url.host
        db = engine.url.database
        engine_str = str(engine.url)
        engine_exists_check = self.session.query(self.Engines).filter_by(db=db, host=host, engine=engine_str).first()
        if engine_exists_check: return None
        self.session.add(self.Engines(db=db, host=host, engine=engine_str))
        self.session.commit()
        
    def _get_engine(self, by: str='host', **kwargs) -> Engine:
        return self.session.query(self.Engines).where(host=by).first()
    
    def register_line_vars(self, line):
        """options: engine, var, bg"""
        mode = self.get_mode(line)
        if line.strip() and mode == 'query':
            line = line.split(' ')
            line_vars = {}
            for var in line:
                key,value = var.split('=')
                line_vars[key] = value
            self.line_vars = line_vars
            return line_vars
        return {}
    
    def push_var(self, obj):
        if hasattr(self, 'line_vars'):
            var = self.line_vars.get('var')
            self.ipy.push({var: obj})
            return True
        
    def run_query(self, engine, query, var=None, **kwargs):
        results = pd.DataFrame([dict(row) for row in engine.execute(*query)])
        if var:
            self.ipy.push({var: results})
        return results
    
    def run_in_background(self, *args):
        pool = ThreadPool(processes=1)
        async_result = pool.apply_async(self.run_query, (*args))
        return async_result
    
    def get_engine(self, engine_var: str, as_binary: bool=False):
        if engine_var:
            if engine_var not in self.db_info:
                engine = create_engine(engine_var) #new engines
                self.add_engine(engine)
            else:
                engine = create_engine(self.db_info[engine_var]) #engine lookup
        else:
            engine = SQLCell.current_engine or self.latest_engine
        return engine
    
    def get_mode(self, line):
        line = [l.split('=') for l in line.split('=')]
        if len(line) == 0:
            if line in SQLCell.modes: return line
            else: raise Exception('Invalid mode, please review docs')
        return 'query'

    def get_bind_params(self, params, ipython):
        return {key:getattr(ipython.user_module, key) for key in params.keys()}

    def get_sql_statement(self, cell):
        text = sql.text(cell)
        params = text.compile().params
        bind_params = self.get_bind_params(params, self.ipy)
        return (text, bind_params)
    
    @line_cell_magic
    def sql(self, line: str="", cell: str="") -> None:
        
        line = line.strip()
        cell = cell.strip()
        line_args = ArgHandler(line).args
        container_var = line_args.var 
        engine_var = line_args.engine
        background = line_args.background
        hook = line_args.hook
        refresh = line_args.refresh
        
        engine = self.get_engine(engine_var)
        ########################## HookHandler logic ########################
        hook_handler = HookHandler(engine)
        if hook:
            if cell == 'list': 
                return hook_handler.list()
            hook_handler.add(line, cell)
            return ('Hook successfully registered')
        
        if cell.startswith(self.hook_indicator):
            engine, cell = hook_handler.run(cell, engine_var)
            SQLCell.current_hook_engine = hook_handler.hook_engine
        ########################## End HookHandler logic ####################
        ############################ Refresh logic ##########################
        if refresh and cell in self.refresh_options:
            if cell in self.tables:
                self.session.query(getattr(self.classes, cell)).delete()
                self.session.commit()
            return ('Removed all records from ' + cell)
        ############################ End Refresh logic ######################
        sql_statemnent_params = self.get_sql_statement(cell)
        results = self.run_query(engine=engine, query=sql_statemnent_params, var=container_var)
        self.push_var(results)
        engine.pool.dispose()
        
        # reinitialize to update db_info, find better way
        self.db_info = SQLCell(self.shell, self.data).db_info 
        SQLCell.current_engine = engine
        return results

def load_ipython_extension(ipython):
    run()
    magics = SQLCell(ipython, [])
    ipython.register_magics(magics)
