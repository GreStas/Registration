if __name__ == "__main__":
    defconnection = {"pg_hostname": "deboraws",
                     "pg_database": "test_db",
                     "pg_user": "tester",
                     "pg_passwd": "testing",
                     "pg_schema": "dev1",
                     "pg_role": "test_db_dev1_users"}

    log_rw1 = logging.getLogger("RegWorker1")
    # dbpool_rw1 = DBpool(defconnection, minconn=1)
    # rr = RegWorker(dbpool_rw1, log_rw1)

    rr = getRegWorker(defconnection, 12, 20)

    ###
    #  Тест для RegWorker.SaveRequest
    ##
    # print "Тест для RegWorker.SaveRequest"
    # request_id = rr.SaveRequest("grestas2000@mail.ru", "GreStas", "DefaultP@w0rd")
    # if request_id is None:
    #     print "main:Unknown SaveRequest result: None"
    # elif request_id < 0:
    #     print "main:SaveRequest for (%s,%s,%s) return errorcode(%d):%s" \
    #           % ("grestas2000@gmail.com",
    #              "GreStas",
    #              "DefaultP@w0rd",
    #              request_id,
    #              RegWorker.ErrMsgs[request_id])
    # else:
    #     print "Request #%d saved successfuly." % request_id
    ###

    ###
    #  Тест для RegWorker.RegApprover
    ##
    # print "Тест для RegWorker.RegApprover"
    # result = rr.RegApprover("d3f037ec6104d9f12872075f1b225e97")
    # print "Approve return", result
    ###

    ###
    #  Тест для RegWorker.Garbage
    ##
    print "Тест для RegWorker.Garbage"
    rr.garbage(60 * 60 * 24 * 2)
    ###

    ###
    #  Тест для RegWorker
    ##
    # (its_p, mine_p) = multiprocessing.Pipe()
    # rprc = RegPrc(defconnection, (its_p, mine_p))
    # rprc.start()
    # request = ("SaveRequest","grestas2000@mail.ru","GreStas","DefaultP@w0rd");print "request=", request
    # mine_p.send(request)
    # request = ("RegApprover","7c0cb2904a30049325add8282f17e42a");print "request=", request
    # mine_p.send(request)
    # request = ("Garbage",600);print "request=", request
    # mine_p.send(request)
    # answer = mine_p.recv();print "answer=", answer
    # mine_p.close();print "mine_p.close()"
    # its_p.close();print "its_p.close()"
    # rprc.terminate()
    ###

    #rr.close()
    # dbpool_rw1.close()
    rr.close_dbpool()
    del rr
