# Standard
from datetime import datetime
from enum import Enum
import os
import sys
from typing import Any, Dict, List
# External
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import RealDictCursor, execute_batch, execute_values
from psycopg2.sql import Identifier as PgI, Literal as PgL, SQL as PgQ

def read_con_args_from_env(env: str):
    con_args = {
        'host':     os.environ[f'{env}_DB_HOST'],
        'port':     os.environ[f'{env}_DB_PORT'],
        'database': os.environ[f'{env}_DB_NAME'],
        'user':     os.environ[f'{env}_DB_USER'],
        'password': os.environ[f'{env}_DB_PASS'],
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

class Fetch(Enum):
    All = -1
    Zro = 0
    One = 1

class PgSQL():

    def __init__(self, con_args: Dict[str, Any] = None, qdir: str = 'sql', env: str = None):
        if env is not None:
            con_args = read_con_args_from_env(env)
        if con_args['password'] == 'IAM':
            from awspy.rds import RDS
            con_args['password'] = RDS().iam_auth(con_args['database'], con_args['port'], con_args['user'])
        self._con_args = con_args
        self._con_last = datetime.now()
        self._con = None
        self._qdir = qdir
        # Catch unhandled exceptions and close the connection
        next_excepthook = sys.excepthook
        def close_on_exception(etype, value, tb):
            print('bye!')
            self.close()
            next_excepthook(etype, value, tb)
        sys.excepthook = close_on_exception

    def _connect(self, con_args: Dict[str, Any] = None):
        if con_args is None:
            con_args = self._con_args
        return psycopg2.connect(**con_args)

    def _exec(self, fetch: Fetch, qname: str, qvars: Dict[str, Any] = None, query: str = None):
        if query is None: query = self.read_query(qname)
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

    def close(self):
        if self._con is None:
            return
        if self._con.closed == 0:
            self._con.close()
        self._con = None

    def connection(self):
        if self._con is None \
                or self._con.closed != 0 \
                or (datetime.now() - self._con_last).total_seconds() > 60:
            self._con = self._connect()
            self._con_last = datetime.now()
        return self._con

    def exec(self, qname: str, qvars: Dict[str, Any] = None, query: str = None):
        return self._exec(Fetch.Zro, qname, qvars = qvars, query = query)

    def exec_batch(self, qname: str, qvars_list: List[Dict[str, Any]], query: str = None):
        if query is None: query = self.read_query(qname)
        con = self.connection()
        with con:
            with con.cursor() as cur:
                execute_batch(self, cur, query, qvars_list)

    def exec_fetch_all(self, qname: str, qvars: Dict[str, Any] = None, query: str = None):
        return self._exec(Fetch.All, qname, qvars = qvars, query = query)

    def exec_fetch_one(self, qname: str, qvars: Dict[str, Any] = None, query: str = None):
        return self._exec(Fetch.One, qname, qvars = qvars, query = query)

    def exec_values(self, qname: str, values, query: str = None):
        if query is None: query = self.read_query(qname)
        con = self.connection()
        with con:
            with con.cursor() as cur:
                execute_values(self, cur, query, values)

    def read_query(self, qname: str):
        if qname[0] in './':
            sql_file = qname
        else:
            sql_file = os.path.join(self._qdir, f'{qname}.sql')
        if not os.path.exists(sql_file):
            raise Exception(f'SQL `{qname}` not defined!')
        with open(sql_file) as f:
            query = f.read()
        return query
