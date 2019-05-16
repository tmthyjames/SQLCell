from db.session import DBSessionHandler

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