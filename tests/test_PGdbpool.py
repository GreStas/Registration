import PGdbpool


if __name__ == "__main__":

    defconnection = {}
    defconnection["pg_hostname"] = "deboraws"
    defconnection["pg_database"] = "test_db"
    defconnection["pg_user"] = "tester"
    defconnection["pg_passwd"] = "testing"
    defconnection["pg_role"] = "test_db_dev1_users"
    defconnection["pg_schema"] = "dev1"

    # pause = 0.001

    pool = DBpool(defconnection, minconn=2, maxconn=4)

    # conn1 = pool.connect(); print "main: conn1 is process #%s" % conn1.name
    # conn2 = pool.connect(); print "main: conn2 is process #%s" % conn2.name
    # conn3 = pool.connect(); print "main: conn3 is process #%s" % conn3.name
    # print "sleeping after 3 connects"
    # time.sleep(pause)

    # conn1.execSimpleSQL("select count(*) from registrations")

    # pool.disconnect(conn1.name)
    # print "main: sleeping after disconnect #1"
    # time.sleep(pause)

    conn4 = pool.connect(); print "main: conn4 is process #%s" % conn4.name

    conn5 = pool.connect(); print "main: conn5 is process #%s" % conn5.name
    # test_sql = "select count(*) from registrations"
    test_sql = "select count(*) from registrations where logname='%s'" % "GreStas"
    print "main.sql = '%s'" % test_sql
    try:

        print "main: Test execSimpleSQL"
        conn4.exec_simple_sql(test_sql)

        print "main: Test execGatherSQL.fetchone()"
        conn5.exec_gather_sql(test_sql)
        rows = conn5.fetch_one()
        for row in rows: print "main fetched: ", row

        # print "main: Test execGatherSQL.fetchmany(20)"
        # conn4.execGatherSQL("select * from registrations")
        # rows = conn4.fetchmany(20)
        # for row in rows: print "main fetched: ", row

        # print "main: Test execGatherSQL.fetchall()"
        # conn5.execGatherSQL("select * from registrations")
        # rows = conn5.fetchall()
        # for row in rows: print "main fetched: ", row

    except Error as e:
        print "main: error "
        e.print_error()

    # pool.disconnect(conn2.name)
    # pool.disconnect(conn3.name)
    # pool.disconnect(conn4.name)
    # pool.disconnect(conn5.name)

    pool.close()
