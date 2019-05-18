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
import threading
################# SQLCell modules #################
from sqlcell.db import EngineHandler, DBSessionHandler
from sqlcell.args import ArgHandler
from sqlcell.hooks import HookHandler
from sqlcell._initdb import run


@magics_class
class SQLCell(Magics, EngineHandler):
    
    current_engine = False
    current_hook_engine = False
    modes = ['query', 'hook', 'refresh']
    # consider yaml file for these types of params:
    hook_indicator = '~'
    
    def __init__(self, shell, data):
        # You must call the parent constructor
        super(SQLCell, self).__init__(shell)
        self.shell = shell
        self.data = data
        self.ipy = get_ipython()
        self.refresh_options = ['hooks', 'engines']
        self.line_args = None
    
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
        if self.line_args.var:
            self.ipy.push({self.line_args.var: obj})

    def async_handler(self, obj):
        self.push_var(obj)
        return obj
        
    def run_query(self, engine, query_params, var=None, callback=None, **kwargs):
        results = pd.DataFrame([dict(row) for row in engine.execute(*query_params)])
        return callback(results)
    
    def query_router(self, *args):
        if self.line_args.background:
            processThread = threading.Thread(target=self.run_query, args=args)
            processThread.start()
            return None
        return self.run_query(*args)
    
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
        add_engines = line_args.engines
        # refer to all args as self.line_args.<arg> to get rid of entire block ^?
        self.line_args = line_args

        ############################ Refresh logic ##########################
        if refresh and cell in self.refresh_options:
            if cell in self.tables:
                self.session.query(getattr(self.classes, cell)).delete()
                self.session.commit()
            return ('Removed all records from ' + cell)
        ############################ End Refresh logic ######################
        ############################ Engine Aliases Logic ###################
        if self.line_args.engines:
            if cell == 'list':
                return self.list()
            else:
                self.add_alias(cell)
                # need to reinit db_info to update new engines added
                self.db_info = SQLCell(self.shell, self.data).db_info 
                return ('Engines successfully registered')
        ############################ End Engine Aliases #####################
        engine = self.get_engine(engine_var) # need engine below but not in refresh or alias logic
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
        sql_statemnent_params = self.get_sql_statement(cell)
        results = self.query_router(engine, sql_statemnent_params, self.line_args.var, self.async_handler)
        # self.push_var(results)
        engine.pool.dispose()
        
        # reinitialize to update db_info, find better way
        self.db_info = SQLCell(self.shell, self.data).db_info 
        SQLCell.current_engine = engine
        return results

def load_ipython_extension(ipython):
    run()
    magics = SQLCell(ipython, [])
    ipython.register_magics(magics)
