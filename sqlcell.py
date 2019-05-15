from IPython.core.magic import (Magics, magics_class, line_magic,
                                cell_magic, line_cell_magic)
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import desc, asc
from sqlalchemy.engine.base import Engine
import pandas as pd
import pickle
from IPython.display import display, Javascript
from ipywidgets import Button, HBox, VBox
from multiprocessing.pool import ThreadPool
import argparse
import shlex


class ArgHandler(object):
    def __init__(self, line):
        self.parser = argparse.ArgumentParser(description='SQLCell arguments')
        self.parser.add_argument(
            "-e", "--engine", 
            help='Engine param, specify your connection string: --engine=postgresql://user:password@localhost:5432/mydatabase', 
            required=False
        )
        self.parser.add_argument(
            "-v", "--var", 
            help='Variable name to write output to: --var=foo', 
            required=False
        )
        self.parser.add_argument(
            "-b", "--background", 
            help='whether to run query in background or not: --background runs in background', 
            required=False, default=False, action="store_true"
        )
        self.parser.add_argument(
            "-k", "--hook", 
            help='define shortcuts with the --hook param',
            required=False, default=False, action="store_true"
        )
        self.parser.add_argument(
            "-r", "--refresh", 
            help='refresh engines by specifying --refresh flag',
            required=False, default=False, action="store_true"
        )
        self.args = self.parser.parse_args(shlex.split(line))

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
            
class HookHandler(DBSessionHandler):
    """input common queries to remember with a key/value pair. ie, 
       %%sql hook
       \d=<common query>"
       \dt=<another common query>"""
    def __init__(self, *args, **kwargs):
        super().__init__()
        
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
        SQLCell.current_hook_engine = hook_engine
        return hook_engine, hook_cmd.format(*cmd_args)
    
    def refresh(self, cell):
        self.session.query(Hooks).delete()
        self.session.commit()

@magics_class
class SQLCell(Magics, DBSessionHandler):
    
    current_engine = False
    current_hook_engine = False
    modes = ['query', 'hook', 'refresh']
    # consider cfg file for these types of params:
    hook_indicator = '~'
    
    def __init__(self, shell, data):
        # You must call the parent constructor
        super().__init__(shell)
        self.shell = shell
        self.data = data
        self.ipy = get_ipython()
        self.refresh_optinos = ['hooks', 'engines']

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
        
    def run_query(self, engine, query, var=None):
        results = pd.DataFrame([dict(row) for row in engine.execute(query)])
        self.ipy.push({var or 'DF': results})
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
        hook_handler = HookHandler()
        if hook:
            hook_handler.add(line, cell)
            return ('Hook successfully registered')
        
        if cell.startswith(self.hook_indicator):
            engine, cell = hook_handler.run(cell, engine_var)
        ########################## End HookHandler logic ####################
        ############################ Refresh logic ##########################
        if refresh and cell in self.refresh_optinos:
            if cell in self.tables:
                self.session.query(getattr(self.classes, cell)).delete()
                self.session.commit()
            return ('Removed all records from ' + cell)
        ############################ End Refresh logic ######################

        results = self.run_query(engine, cell, container_var)
        
        self.push_var(results)
        engine.pool.dispose()
        
        # reinitialize to update db_info, find better way
        self.db_info = SQLCell(self.shell, self.data).db_info 
        SQLCell.current_engine = engine
        return results
    
class TabCompleter(DBSessionHandler):
    pass
    
class BackgroundHandler(object):
    """bg=True"""
    pass

class ControlPanel(object):
    # def run_btn_callback(evt): # figure out how to execute a cell
    #     Javascript("""
    #         var CodeCell = __webpack_require__(/*! @jupyterlab/cells */ "USP6").CodeCell
    #         CodeCell.execute()
    #     """)


    # def get_control_panel(): # remove until I can get JS working
    #     run_btn = Button(description='run')
    #     run_btn.on_click(run_btn_callback)
    #     stop_btn = Button(description='stop')
    #     bg_btn = Button(description='bg')

    #     left_box = VBox([run_btn])
    #     center_box = VBox([stop_btn])
    #     right_box = VBox([bg_btn])
    #     control_panel = HBox([left_box, center_box, right_box])
    #     return control_panel
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


def load_ipython_extension(ipython):
    magics = SQLCell(ipython, [])
    ipython.register_magics(magics)
