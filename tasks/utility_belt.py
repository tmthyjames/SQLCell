import threading
import csv
import re
from sqlalchemy import create_engine
from IPython.display import display, Javascript

from ..python_js.interface_js import load_js_scripts

def threaded(fn):
    def wrapper(*args, **kwargs):
        threading.Thread(target=fn, args=args, kwargs=kwargs).start()
    return wrapper

class HTMLTable(list):
    """
    Creates an HTML table if pandas isn't installed.
    The .empty attribute takes the place of df.empty,
    and to_csv takes the place of df.to_csv.
    """

    def __init__(self, data, id_):
        self.id_ = id_
        self.data = data

    empty = []

    def _repr_html_(self, n_rows=100, length=100, edit=False):
        table = '<table id="table'+self.id_+'" width=100%>'
        thead = '<thead><tr>'
        tbody = '<tbody>'
        j = 48
        query_plan = False
        for n,row in enumerate(self.data):
            if n == 0:
                if list(row):
                    query_plan = True if row[0] == 'QUERY PLAN' else False
                    if query_plan:
                        execution_time = re.findall('[0-9]{,}\.[0-9]{,}', str(self.data[-1][0]))
                        execution_time = execution_time if not execution_time else float(execution_time[0])
                    thead += '<th>' + ' ' + '</th>' ''.join([('<th>' + str(r) + '</th>') for r in row])
            elif n > n_rows:
                if not query_plan:
                    break
            else:
                if not query_plan:
                    if n > 50 and length > 100:
                        n = length - j
                        j -= 1
                    tbody += '<tr class="text-nowrap"><td>' + str(n) + '</td>' + ''.join([('<td tabindex="1" data-column="'+str(r).replace('\\', '\\\\')+'">' + str(r).replace('\\', '\\\\') + '</td>') for r in row]) + '</tr>'
                else:
                    section_time = re.search('actual time=([0-9]{,}\.[0-9]{,})\.\.([0-9]{,}\.[0-9]{,})', str(row[0]))
                    background_color = ""

                    if section_time:
                        start_time = float(section_time.group(1))
                        stop_time = float(section_time.group(2))

                        if (stop_time - start_time) > (execution_time * 0.9):
                            background_color = "#800026"
                        elif (stop_time - start_time) > (execution_time * 0.8):
                            background_color = "#bd0026"
                        elif (stop_time - start_time) > (execution_time * 0.7):
                            background_color = "#e31a1c"
                        elif (stop_time - start_time) > (execution_time * 0.6):
                            background_color = "#fc4e2a"
                        elif (stop_time - start_time) > (execution_time * 0.5):
                            background_color = "#fd8d3c"
                        elif (stop_time - start_time) > (execution_time * 0.4):
                            background_color = "#feb24c"
                        elif (stop_time - start_time) > (execution_time * 0.3):
                            background_color = "#fed976"
                        elif (stop_time - start_time) > (execution_time * 0.2):
                            background_color = "#ffeda0"
                        elif (stop_time - start_time) > (execution_time * 0.1):
                            background_color = "#ffffcc"
                        else:
                            background_color = ""

                    td_row = '<tr><td>' + str(n) + '</td>' + ''.join([('<td>' + str(r).replace('  ', '&nbsp;&nbsp;&nbsp;') + '</td>') for r in row]) + '</tr>'
                    repl = '<b style="background-color:{color};">actual time</b>'.format(color=background_color)
                    td_row = re.sub('actual time', repl, td_row)
                    tbody += td_row
        # tbody += '<tr style="height:40px;">' + ''.join([('<td></td>') for r in row]) + '</tr>' # for adding new row
        thead += '</tr></thead>'
        tbody += '</tbody>'
        table += thead + tbody
        return table

    @threaded
    def display(self, columns=[], msg=None):
        data = self.data if len(self.data) <= 100 else self.data[:49] + [['...'] * (len(self.data[0]))] + self.data[-49:]
        table_str = HTMLTable([columns] + data, self.id_)._repr_html_(n_rows=100, length=len(self.data))
        table_str = table_str.replace('<table', '<table class="table-striped table-hover table-bordered"').replace("'", "\\'").replace('\n','')
        display(
            HTML(
                """
                <script type="text/Javascript">
                $('#dbinfo{id}').append('{msg}');
                $('#table{id}').append('{table}');
                </script>
                """.format(msg=str(msg), table=table_str, id=self.id_)
            )
        )

    def to_csv(self, path):
        with open(path, 'w') as fp:
            a = csv.writer(fp, delimiter=',')
            a.writerows(self.data)


def build_dict(output, row, __KERNEL_VARS__):
    output[row.replace('%(','').replace(')s','')] = eval("__KERNEL_VARS__.get('"+row.replace('%(','').replace(')s','')+"')")
    return output


def kill_last_pid(app=None, db=None): 

    connection = create_engine("postgresql://tdobbins:tdobbins@localhost:5432/"+db+"?application_name=garbage_collection")
    try:
        pid_sql = """
            SELECT pid 
            FROM pg_stat_activity 
            where application_name = %(app)s
            """
        pids = [i.pid for i in connection.execute(pid_sql, {
                'app': app
            }
        )]
        for pid in pids:
            cancel_sql = "select pg_cancel_backend(%(pid)s);"
            cancel_execute = [i for i in connection.execute(cancel_sql, {
                    'pid': pid
                }
            )]
            print 'cancelled postgres job:', pid, 'application: ', app

        return True

    except Exception as e:
        print e
        return False

    finally:
        print 'closing DB connection....'
        connection.dispose()

    return True

class ParseNodes(object):

    def __init__(self, obj):
        self.obj = obj

    def get_depth(self, itr=0, depth=[]):
        if isinstance(self.obj, dict):
            for k, v2 in self.obj.items():
                if 'Plan' in k:
                    if k == 'Plans':
                        itr += 1
                        depth.append(itr)
                    ParseNodes(v2).get_depth(itr=itr, depth=depth)
        elif isinstance(self.obj, list):
            for i, v2 in enumerate(self.obj):
                if 'Plans' in v2:
                    ParseNodes(v2).get_depth(itr=itr, depth=depth)
        else:
            depth.append(itr)
        return depth

    @staticmethod
    def build_node(id_, node, xPos):
        _node = {
            'name': id_,
            'nodetype': node.get('Plan', node).get('Node Type'),
            'starttime': node.get('Plan', node).get('Actual Startup Time'),
            'endtime': node.get('Plan', node).get('Actual Total Time'),
            'subplan': node.get('Plan', node).get('Subplan Name'),
            'display': str(node.get('Plan', node).get('Join Filter', 
                                                  node.get('Filter', 
                                                           node.get('Index Cond', 
                                                                    node.get('Hash Cond', 
                                                                             node.get('One-Time Filter',
                                                                                     node.get('Recheck Cond',
                                                                                             node.get('Group Key')
                                                                                             )
                                                                                     )
                                                                            )
                                                                   )
                                                          )
                                                 ) or '') + (' using ' 
                                            + str(node.get('Index Name', 
                                                           node.get('Relation Name',
                                                                   node.get('Schema')))) + ' ' + str(node.get('Alias')or'')
                                                if node.get('Index Name', 
                                                            node.get('Relation Name',
                                                                    node.get('Schema'))) 
                                                else ''),
            'rows': node.get('Plan', node).get('Plan Rows'),
            'xPos': xPos
        }
        return _node

    def node_walk(self, key, nodes={}, xPos=None):
        if not nodes.get('nodes'):
            nodes['nodes'] = []
            nodes['links'] = []
            nodes['executionTime'] = self.obj.get('Execution Time')
            nodes['depth'] = 0
        target = id(self.obj)
        source_node = ParseNodes.build_node(target, self.obj, xPos)
        xPos -= 1
        if source_node not in nodes['nodes']:
            nodes['nodes'].append(source_node)
        for i in self.obj.get('Plan', self.obj)[key]:
            source = id(i)
            if isinstance(i, dict):
                plans = i.get('Plans')
                target_node = ParseNodes.build_node(source, i, xPos)
                if target_node not in nodes['nodes']:
                    nodes['nodes'].append(target_node)
                nodes['links'].append({'source':source, 'target':target,'value':i.get('Total Cost')})
                if plans:
                    nodes['depth'] += 1
                    
                    ParseNodes(i).node_walk('Plans', nodes, xPos)
        return nodes

def load_js_files():
    display(Javascript(
        load_js_scripts()
    ))
    return None