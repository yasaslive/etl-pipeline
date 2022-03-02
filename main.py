"""
Created on Fri Feb 25 02:19:26 2022.

@author: Yasas
"""
# -*- coding: utf-8 -*-

from multiprocessing import Pool
import configparser
import pandas as pd
import mysql.connector
import pyodbc


def execute_extraction(table: str, config_mysql: dict, config_mssql: dict, direction, *args, chunk_size=1000, **kwargs):
    """
    Select and insert data to the Mysql or MSSQL server.

    Parameters
    ----------
    table : TYPE
        Table name of the sql db table.
    config_mysql : dict
        mysql config parmas.
    config_mssql : dict
        mssql config params.
    direction : TYPE, optional
        Direction of data flow. The default is 'SM'.
    chunk_size : TYPE, optional
        Chunk size for df to operate. The default is 1000.
    *args : TYPE
        Optional args for future use.
    **kwargs : TYPE
        Optional args for future use.

    Returns
    -------
    bool
        Returns true.

    """
    try:
        con_mysql = mysql.connector.connect(config_mysql)
        con_mssql = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+config_mysql.get('host')+';DATABASE=' +
                                   config_mssql.get('database')+';UID='+config_mssql.get('username')+';PWD=' + config_mssql.get('password'))
        if direction == 'SM':
            data_iterator = pd.read_sql_table(table, con_mssql, chunksize=chunk_size)
            for df in data_iterator:
                df.to_sql(table, con_mysql)
        else:
            data_iterator = pd.read_sql_table(table, con_mysql, chunksize=chunk_size)
            for df in data_iterator:
                df.to_sql(table, con_mssql)
    except mysql.connector.DatabaseError as db_error:
        print(db_error)
    finally:
        con_mysql.close()
        con_mssql.close()

    return True


def pipeline(config):
    """
    Process pool for each table in the db.

    Parameters
    ----------
    config : TYPE
        Config parser for db params.

    Returns
    -------
    None.

    """
    try:
        conf_mysql = {'host': config['mysql']['host'], 'database': config['mysql']['database'], 'user': config['mysql']
                      ['user'], 'password': config['mysql']['password'], 'port': config['mysql']['port']}
        conf_mssql = {'host': {config['mssql']['host']}, 'database': config['mssql']['database'], 'user': config['mssql']
                      ['user'], 'password': config['mssql']['password'], 'port': config['mssql']['port']}

        con_mysql = mysql.connector.connect(**conf_mysql)
        con_mssql = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+conf_mysql.get('host')+';DATABASE=' +
                                   conf_mssql.get('database')+';UID='+conf_mssql.get('username')+';PWD=' + conf_mssql.get('password'))
        if config['direction']['data_direction'] == 'SM':
            cursor = con_mssql.cursor()
            cursor.execute('SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES')
            ir = cursor.fetchall()

        if config['direction']['data_direction'] == 'MS':
            cursor = con_mysql.cursor()
            cursor.execute('SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = %s', config['mysql']['database'])
            ir = cursor.fetchall()
        '''
        with Pool() as pool:

            xar = [(x, conf_mysql, conf_mssql) for x in range(10)]
            result = pool.starmap_async(execute_extraction, xar)
            print(result)'''
    except mysql.connector.DatabaseError as mysql_db_error:
        print(mysql_db_error)
    except pyodbc.DatabaseError as mssql_db_error:
        print(mssql_db_error)
    finally:
        con_mysql.close()
        con_mssql.close()
        cursor.close()


def main():
    """
    Program entry point.

    Returns
    -------
    None.

    """
    config = configparser.ConfigParser()
    config.read('config.ini')
    pipeline(config)


if __name__ == '__main__':
    main()
