import redshift_connector


def access_redshift():
    conn = redshift_connector.connect(
        host='redshift-test.cyernhele58c.ap-northeast-2.redshift.amazonaws.com',
        database='redshift_test',
        user='testuser',
        password='Testpw1234')

    cursor = conn.cursor()
    cursor.execute("insert into users values (3, 'cc')")
    #result = cursor.fetchall()
    #print('result : ', result)
