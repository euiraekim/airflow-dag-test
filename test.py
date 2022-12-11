from functions import get_redshift_conn, get_postgresql_conn

insert_query = "insert into sign_up_count values ('aaa');"

select_query = """
    select * from test;
"""

redshift_conn = get_redshift_conn()

postgresql_conn = get_postgresql_conn()

with redshift_conn.cursor() as cursor:
    try:
        cursor.execute(select_query)
        rows = cursor.fetchall()
        print('rows'+ str(rows))
    except Exception as e:
        print(e)
