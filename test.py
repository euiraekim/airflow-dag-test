from connection import get_redshift_conn, get_postgresql_conn

insert_query = "insert into sign_up_count values ('aaa');"

select_query = """
    select * from sign_up_count;
"""

conn = get_postgresql_conn()

with conn.cursor() as cursor:
    try:
        cursor.execute(select_query)
        rows = cursor.fetchall()
        print('rows'+ str(rows))
    except Exception as e:
        print(e)
