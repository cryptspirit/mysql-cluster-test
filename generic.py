import unittest
import MySQLdb
from _mysql_exceptions import OperationalError
from os import fork
from time import sleep, time
from contextlib import closing
from uuid import uuid4

def spin(count, pid=1):
    if pid == 0:
        return count + 1
    if count < 1:
        return 0
    else:
        return spin(count - 1, fork())

mysql_port = 3306
mysql_user = 'pyuser'
mysql_password = 'pypassword'
mysql_db = 'pydb'
mysql_table = 'pytable'

mysql_pool = [
    {
        'host': '10.0.2.87',
        'port': mysql_port,
        'user': mysql_user,
        'passwd': mysql_password,
        'db': mysql_db,
        'use_unicode': 1,
        'charset': 'utf8',
    },
    {
        'host': '10.0.2.98',
        'port': mysql_port,
        'user': mysql_user,
        'passwd': mysql_password,
        'db': mysql_db,
        'use_unicode': 1,
        'charset': 'utf8',
    },
]

create_table = r'''
CREATE TABLE %s (
    id MEDIUMINT NOT NULL AUTO_INCREMENT,
    uuid VARCHAR(128) NOT NULL,
    comments VARCHAR(1024),
    write_from VARCHAR(1024) NOT NULL,
    PRIMARY KEY  (`id`)
)
'''
create_index1 = r'''
CREATE UNIQUE INDEX %s ON %s (%s)
'''

def mysql_connection(mysql_coroutine):
    def __wrap__(*args, **kargs):
        try:
            return mysql_coroutine(*args, **kargs)
        except OperationalError, e:
            if e[0] == 1213:
                return mysql_coroutine(*args, **kargs)
            else:
                args[0].fail(e)
    return __wrap__

class Test(unittest.TestCase):
    def roundrobin(self, i=None):
        if not i: i = self.id
        l = len(self.dbs) - 1
        if i + 1 <= l:
            j = i + 1
        else:
            j = 0
        return j

    @mysql_connection
    def master(self, query, commit=False):
        ''' Query from master
        '''
        db = self.db()
        cursor = db.cursor()
        r = cursor.execute(query)
        if commit:
            db.commit()
        cursor.close()
        db.close()
        return r

    @mysql_connection
    def slave(self, query, commit=False, i=None):
        ''' Query from slave or other server
        '''
        if not i: i = self.roundrobin()
        db = self.dbs[i]()
        cursor = db.cursor()
        r = cursor.execute(query)
        if commit:
            db.commit()
        cursor.close()
        db.close()
        return r

    def _current(self):
        l = len(mysql_pool) - 1
        if l:
            i = spin(count=l)
        else:
            i = 0
        self.id = i

    def _prepare(self):
        ''' Prepaire DB for test
        '''
        try:
            db = MySQLdb.connect(**mysql_pool[0])
            cursor = db.cursor()
        except OperationalError, e:
            self.fail(e)
        else:
            cursor.execute('''DROP TABLE IF EXISTS %s''' % mysql_table)
            cursor.execute(create_table % mysql_table)
            cursor.execute(create_index1 % ('uuid', mysql_table, 'uuid'))
        cursor.close()
        db.close()

    def setUp(self):
        self.uuid = uuid4()
        self._prepare()
        self._current()
        self.mysql = mysql_pool
        self.dbs = []
        for j in mysql_pool:
            self.dbs.append(lambda: MySQLdb.connect(**j))

        self.db = self.dbs[self.id]
        self.table = mysql_table


    def tearDown(self):
        pass
        #for i in self.dbs:
        #    i.close()
