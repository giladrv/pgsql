# Standard
from datetime import datetime
from enum import Enum
import os
import sys
import time
from traceback import print_exception
from typing import Any, Dict, List, Tuple
# External
import psycopg2
from psycopg2.errors import OperationalError
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import RealDictCursor, execute_batch, execute_values
from psycopg2.sql import Identifier as PgI, SQL as PgQ

MAX_ATTEMPTS = 3

def do_iam_auth(con_args: Dict[str, Any]):
    if con_args['password'] == 'IAM':
        from awspy.rds import RDS
        token = RDS().iam_auth(con_args['host'], con_args['port'], con_args['user'])
        con_args['password'] = token

def do_sem_auth(con_args: Dict[str, Any]):
    if con_args['password'] == 'SEM':
        from awspy.sem import SEM
        secret = SEM().get_secret_json(con_args['user'])
        con_args['user'] = secret['username']
        con_args['password'] = secret['password']

def read_con_args_from_env(env: str):
    con_args = {
        'host':     os.environ[f'{env}_DB_HOST'],
        'port':     int(os.environ[f'{env}_DB_PORT']),
        'user':     os.environ[f'{env}_DB_USER'],
        'password': os.environ[f'{env}_DB_PASS'],
        'database': os.environ[f'{env}_DB_NAME'],
    }
    if f'{env}_DB_APP' in os.environ:
        con_args['application_name'] = os.environ[f'{env}_DB_APP'],
    return con_args

def read_query(qdir: str, qname: str):
    sql_file = f'{qname}.sql'
    if not sql_file.startswith('/') and qdir is not None:
        sql_file = os.path.join(qdir, f'{qname}.sql')
    if not os.path.exists(sql_file):
        raise Exception(f'SQL `{qname}` not defined!')
    with open(sql_file) as f:
        query = f.read()
    return query

def sql_table_as_json(table: str, columns: Tuple[str] = ('id', 'name')):
    obj_def = ', '.join([ f''''{col}', "{col}"''' for col in columns ])
    return f'''{table} AS (SELECT jsonb_agg(jsonb_build_object({obj_def}) as "{table}")'''

class Fetch(Enum):
    All = -1
    Zro = 0
    One = 1

class PgSQL():

    def __init__(self, con_args: Dict[str, Any] | None = None, qdir: str = 'sql', env: str | None = None):
        if env is not None:
            con_args = read_con_args_from_env(env)
        self.con = None
        self.orig_con_args = con_args
        self.con_args = con_args.copy()
        self.con_last = datetime.now()
        self.qdir = qdir
        self.tunnel = None
        # Catch unhandled exceptions and close the connection
        next_excepthook = sys.excepthook
        def close_on_exception(etype, value, tb):
            print('bye!')
            self.disconnect()
            next_excepthook(etype, value, tb)
        sys.excepthook = close_on_exception

    def _exec(self, fetch: Fetch, qname: str, qvars: Dict[str, Any] | None = None, query: str | None = None):
        if query is None:
            query = self.read_query(qname)
        attempts = MAX_ATTEMPTS
        while attempts > 0:
            try:
                con = self.connection()
                with con:
                    with con.cursor(cursor_factory = RealDictCursor) as cur:
                        cur.execute(query, qvars)
                        if fetch == fetch.All:
                            res = cur.fetchall()
                        elif fetch == fetch.One:
                            res = cur.fetchone()
                        else:
                            res = None
                return res
            except OperationalError as e:
                print_exception(e)
                if attempts == 1:
                    raise
                self.disconnect()
                time.sleep(8 ** (MAX_ATTEMPTS - attempts))
                attempts -= 1

    def connect(self, con_args: Dict[str, Any] | None = None):
        if con_args is None:
            con_args = self.con_args.copy()
        do_iam_auth(con_args)
        do_sem_auth(con_args)
        if self.tunnel is not None:
            con_args['host'] = self.tunnel.local_bind_host
            con_args['port'] = self.tunnel.local_bind_port
        return psycopg2.connect(**con_args)

    def connection(self):
        if self.con is None \
                or self.con.closed != 0 \
                or (datetime.now() - self.con_last).total_seconds() > 60:
            self.con = self.connect()
            self.con_last = datetime.now()
        return self.con

    def create_db(self, name: str | None = None):
        if name is None:
            name = self.con_args['database']
        con_args = self.con_args.copy()
        con_args['database'] = 'postgres'
        con = self.connect(con_args)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        query = PgQ('CREATE DATABASE {db_name}')
        qvars = { 'db_name': PgI(name) }
        cur.execute(query.format(**qvars))
        cur.close()
        con.close()

    def create_iam_user(self, db_user: str, db_pass: str):
        iam_user = self.con_args['user']
        con_args = self.con_args.copy()
        con_args['user'] = db_user
        con_args['password'] = db_pass
        con_args['database'] = 'postgres'
        con = self.connect(con_args)
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        query = "CREATE USER {iam_user} WITH LOGIN CREATEDB; GRANT rds_iam TO {iam_user};"
        cur.execute(PgQ(query).format(iam_user = PgI(iam_user)))
        cur.close()
        con.close()

    def disconnect(self):
        self.con_args = self.orig_con_args.copy()
        if self.con is None:
            return
        if self.con.closed == 0:
            self.con.close()
        self.con = None

    def exec(self, qname: str, qvars: Dict[str, Any] = None, query: str = None):
        return self._exec(Fetch.Zro, qname, qvars = qvars, query = query)

    def exec_batch(self, qname: str, qvars_list: List[Dict[str, Any]], query: str | None = None):
        if query is None: query = self.read_query(qname)
        con = self.connection()
        with con:
            with con.cursor() as cur:
                execute_batch(self, cur, query, qvars_list)

    def exec_fetch_all(self, qname: str, qvars: Dict[str, Any] = None, query: str | None = None):
        return self._exec(Fetch.All, qname, qvars = qvars, query = query)

    def exec_fetch_one(self, qname: str, qvars: Dict[str, Any] = None, query: str | None = None):
        return self._exec(Fetch.One, qname, qvars = qvars, query = query)

    def exec_values(self, qname: str, values, query: str | None = None):
        if query is None: query = self.read_query(qname)
        con = self.connection()
        with con:
            with con.cursor() as cur:
                execute_values(self, cur, query, values)

    def get_tables_as_json(self, *tables: Tuple[str]):
        q_with = ', '.join(map(sql_table_as_json, tables))
        q_from = ', '.join(tables)
        query = f'WITH {q_with} SELECT * FROM {q_from};'
        return self.exec_fetch_one(None, query = query)

    def read_query(self, qname: str):
        if qname[0] in './':
            sql_file = qname
        else:
            sql_file = os.path.join(self.qdir, f'{qname}.sql')
        if not os.path.exists(sql_file):
            raise Exception(f'SQL `{qname}` not defined!')
        with open(sql_file) as f:
            query = f.read()
        return query

    def tunnel_start(self, ssh_host: str, ssh_user: str, ssh_pkey: str):
        from sshtunnel import SSHTunnelForwarder
        self.tunnel = SSHTunnelForwarder(ssh_host,
            ssh_username = ssh_user,
            ssh_pkey = ssh_pkey,
            remote_bind_address = (self.con_args['host'], self.con_args['port'])
        )
        self.tunnel.start()

    def tunnel_stop(self):
        self.disconnect()
        self.tunnel.stop()
        self.tunnel = None

    def verify_connection(self, msg: str):
        try:
            self.connect()
        except Exception as e:
            if msg in str(e):
                return False
            raise
        return True

    def verify_db_created(self):
        return self.verify_connection(f'database "{self.con_args["database"]}" does not exist')

    def verify_iam_user(self):
        return self.verify_connection('password auth')
