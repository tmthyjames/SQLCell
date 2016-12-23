import threading
import logging
import uuid
from sqlalchemy import create_engine

from ..engines.engine_config import driver, username, password, host, port, default_db
from ..engines.engines import __ENGINES_JSON_DUMPS__, __ENGINES_JSON__
from .utility_belt import kill_last_pid, HTMLTable

unique_db_id = str(uuid.uuid4())
jupyter_id = 'jupyter' + unique_db_id
application_name = '?application_name='+jupyter_id

engine = create_engine(driver+'://'+username+':'+password+'@'+host+':'+port+'/'+default_db+application_name)
conn_string = engine.url

class __SQLCell_GLOBAL_VARS__(object):

    jupyter_id = jupyter_id
    ENGINE = str(engine.url)
    engine = engine
    EDIT = False
    ENGINES = __ENGINES_JSON__
    __EXPLAIN_GRAPH__ = False
    __ENGINES_JSON_DUMPS__ = __ENGINES_JSON_DUMPS__
    DB = default_db
    ISOLATION_LEVEL = 1
    TRANSACTION_BLOCK = True
    INITIAL_QUERY = True
    PATH = False
    RAW = False
    NOTIFY = True

    logger = logging.getLogger()
    handler = logging.StreamHandler()
    logger.setLevel(logging.DEBUG)

    def kill_last_pid_on_new_thread(self, app, db, unique_id):
        t = threading.Thread(target=kill_last_pid, args=(app, db))
        t.start()
        HTMLTable([], unique_id).display(msg='<h5 id="error" style="color:#d9534f;">QUERY DEAD...</h5>')
        return True

    @staticmethod
    def update_table(sql):
        return __SQLCell_GLOBAL_VARS__.engine.execute(sql)