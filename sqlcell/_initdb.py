import sqlite3
#  sqlcell table

db_file = 'sqlcell.db'

create_table_sql = """CREATE TABLE IF NOT EXISTS sqlcell (
 id integer PRIMARY KEY,
 key text NOT NULL,
 value BINARY NOT NULL
);"""

create_engines_sql = """CREATE TABLE IF NOT EXISTS engines (
 id integer PRIMARY KEY,
 db text NOT NULL,
 host text NOT NULL,
 engine text NOT NULL,
 engine_b blob,
 dt datetime default current_timestamp
);"""

create_hooks_sql = """CREATE TABLE IF NOT EXISTS hooks (
 id integer PRIMARY KEY,
 key text NOT NULL UNIQUE,
 cmd text NOT NULL,
 engine text NOT NULL,
 engine_b blob,
 dt datetime default current_timestamp,
 UNIQUE (key, engine) ON CONFLICT IGNORE
);
"""

tables = [create_table_sql, create_engines_sql, create_hooks_sql]

def run():
    """
        initialize sqlite3 database to record engines/hooks
    """
    conn = sqlite3.connect(db_file)
    for table in tables:
        conn.execute(table)

if __name__ == '__main__':
    run()
