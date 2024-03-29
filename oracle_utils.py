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
    _has_obj_sql = "select count(*) from user_objects where object_name ='{}'"
    _has_table_sql = "select count(*) from user_tables where table_name =upper('{}')"

    def __init__(self,
                 username: str,
                 password: str,
                 host: str,
                 port: str,
                 service_name: str = None,
                 sid: str = None):

        dns = None
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

    def has_table(self, table_name: str) -> bool:
        """
            该用户下是否存在表table_name
        """
        sql = self._has_table_sql.format(table_name.upper())

        return self.fetchone(sql)[0] == 1

    def has_data(self, table_name) -> bool:
        """
            表 table_name是否有数据
        """
        sql = f"select count(*) from {table_name} where rownum < 10"

        return self.fetchone(sql)[0] > 0

    def has_object(self, object_name: str) -> bool:
        """
            判断对象是否存在
        """
        sql = self._has_obj_sql.format(object_name.upper())
        return self.fetchone(sql)[0] > 0

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
