# -*- encoding: utf-8 -*-

"""
------------------------------------------
@File       : example.py
@Author     : maixiaochai
@Email      : maixiaochai@outlook.com
@CreatedOn  : 2021/2/26 14:27
------------------------------------------
"""
from oracle_utils import OracleUtils


def main():
    db_cfg = {
        'username': 'maixiaochai',
        'password': 'maixiaochai',
        'host': '192.168.158.1',
        'port': 1521,
        'service_name': 'maixiaochai'
    }

    db = OracleUtils(**db_cfg)
    sql = 'select sysdate from dual'

    result = db.fetchone(sql)
    print(result)


if __name__ == '__main__':
    main()
