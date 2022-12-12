from functions import get_redshift_conn, get_postgresql_conn

insert_query = "insert into sign_up_count values ('aaa');"

select_query = """
    select * from sign_up_count;
"""

user_count_query = """
SELECT count(*)
FROM users
WHERE created_date >= '2022-12-11 07:00:00' and created_date < '2022-12-11 08:00:00';
"""

redshift_conn = get_redshift_conn()

postgresql_conn = get_postgresql_conn()

with redshift_conn.cursor() as cursor:
    try:
        cursor.execute(user_count_query)
        rows = cursor.fetchall()
        print('rows'+ str(rows))
    except Exception as e:
        print(e)
