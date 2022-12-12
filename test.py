from functions import get_redshift_conn, get_postgresql_conn

from datetime import datetime, timedelta


def insert_query(query, db, dt):
    if db == 'redshift':
        conn = get_redshift_conn()
    else:
        conn = get_postgresql_conn()
    
    with conn.cursor() as cursor:
        try:
            cursor.execute(query)
        except Exception as e:
            print(e)

def select_query(query, db, dt):
    if db == 'redshift':
        conn = get_redshift_conn()
    else:
        conn = get_postgresql_conn()

    with conn.cursor() as cursor:
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            return rows
        except Exception as e:
            print(e)


def get_query_dt(execution_date):
    dt = datetime.strptime(execution_date, '%Y-%m-%d %H:%M:%S')
    dt = dt.replace(minute=0, second=0)
    return dt

def sign_up_count_task(execution_date):
    dt = get_query_dt(execution_date)

    redshift_query = f"""
        SELECT COUNT(*) FROM users
        WHERE created_date >= '{dt}' and created_date < '{dt+timedelta(hours=1)}'
    """
    rows = select_query(redshift_query, 'redshift', dt)
    for row in rows:
        print(row)


execution_date = '2022-12-12 02:10:00'
sign_up_count_task(execution_date)
