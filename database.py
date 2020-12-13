import mysql.connector

import config


def select_data(stmt: str, connection: dict=config.DB_MIS) -> object:
    """
    :param stmt:
    :param conection:
    :return:
    :rtype: dict || Exception, bool
    """
    conn = None
    try:
        conn = mysql.connector.connect(**connection)
        cur = conn.cursor(dictionary=True)
        cur.execute(stmt)
        results = cur.fetchall()
        # cur.close()
        # conn.close()
        return results, True
    except Exception as e:
        return e, False
    finally:
        if conn:
            conn.close()


def get_id_list(stmt: str, id_col: str='id', conection: dict=config.DB_MIS):
    """
    :param stmt:
    :param conection:
    :param id_col:
    :return:
    :rtype: list || Exception, bool
    """
    id_list = list()
    conn = None
    try:
        conn = mysql.connector.connect(**conection)
        cur = conn.cursor(dictionary=True)
        cur.execute(stmt)
        id_list_dict = cur.fetchall()
        # cur.close()
        if id_list_dict:
            id_list = [val.get(id_col) for val in id_list_dict]
        return id_list, True
    except Exception as e:
        return e, False
    finally:
        if conn:
            conn.close()


def get_record(stmt: str, conection: dict=config.DB_MIS):
    """
    :param stmt: raw SQL query
    :param conection:
    :return: result of execution query (0..1)
    :rtype: list || Exception, bool
    """
    conn = None
    try:
        conn = mysql.connector.connect(**conection)
        cur = conn.cursor(dictionary=True)
        cur.execute(stmt)
        result = cur.fetchone()
        # cur.close()
        return result, True
    except Exception as e:
        return e, False
    finally:
        if conn:
            conn.close()


def get_record_by_id(table_name: str, table_id: int, connection: dict):

    STMT = """SELECT * FROM `{table_name}` WHERE id = {table_id} LIMIT 1;""".format(table_name=table_name, table_id=table_id)
    record, err = get_record(STMT, connection)
    return (record, err) if record else ({}, err)


def update_data(stmt: str, conection: dict=config.DB_MIS):
    conn = None
    try:
        conn = mysql.connector.connect(**conection)
        cur = conn.cursor()
        cur.execute(stmt)
        conn.commit()
        id = cur.lastrowid
        cur.close()
        conn.close()
        return id, True
    except Exception as e:
        print(stmt)
        print('\n\n')
        print(e)
        return e, False
    finally:
        if conn:
            conn.close()
