# -*- coding: utf-8 -*-

"""
--------------------------------------
@File       : oracle_utils.py
@Author     : maixiaochai
@Email      : maixiaochai@outlook.com
@CreatedOn  : 2020/10/30 16:48
--------------------------------------
"""
from cx_Oracle import makedsn, connect


class OracleUtils:
    def __init__(self,
                 username: str,
                 password: str,
                 host: str,
                 port: str,
                 service_name: str = None,
                 sid: str = None):

        dns = ''
        if service_name:
            dns = makedsn(host, port, service_name=service_name)

        elif sid:
            dns = makedsn(host, port, sid=sid)

        self.conn = connect(username, password, dns)
        self.cur = self.conn.cursor()

    def execute(self, sql, args=None):
        if args:
            self.cur.execute(sql, args)
        else:
            self.cur.execute(sql)

    def executemany(self, sql, args):
        self.cur.executemany(sql, args)
        self.commit()

    def fetchall(self, sql, args=None):
        self.execute(sql, args)
        result = self.cur.fetchall()

        return result

    def fetchone(self, sql, args=None):
        self.execute(sql, args)
        result = self.cur.fetchone()

        return result

    def call_proc(self, proc: str):
        """执行不带参数的存储过程，"""
        proc = proc.strip()
        self.cur.callproc(proc)
        self.commit()

    def has_table(self, table_name):
        """
            该用户下是否存在表table_name
        """
        sql = f"select count(*) from user_tables where table_name =upper('{table_name}')"

        return self.fetchone(sql)[0] == 1

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        try:
            self.cur.close()
            self.conn.close()
        except Exception as err:
            print(str(err))

    def __del__(self):
        self.close()
