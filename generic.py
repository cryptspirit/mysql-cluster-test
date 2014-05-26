import unittest
import MySQLdb
from _mysql_exceptions import OperationalError
from os import fork
from time import sleep, time

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
        'host': 'test4.tr.cslab',
        'port': mysql_port,
        'user': mysql_user,
        'passwd': mysql_password,
        'db': mysql_db,
        'use_unicode': 1,
        'charset': 'utf8',
    },
    {
        'host': 'test5.tr.cslab',
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
    comments VARCHAR(1024) NOT NULL,
    write_from VARCHAR(1024) NOT NULL,
    PRIMARY KEY  (`id`)
)
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
        r = self.cursor.execute(query)
        if commit:
            self.db.commit()
        return r

    @mysql_connection
    def slave(self, query, commit=False, i=None):
        ''' Query from slave or other server
        '''
        if not i: i = self.roundrobin()
        r = self.cursors[i].execute(query)
        if commit:
            self.dbs[i].commit()
        return r

    def _current(self):
        l = len(mysql_pool) - 1
        if l:
            i = spin(count=l)
        else:
            i = 0
        self.id = i

    def _prepaire(self):
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
        cursor.close()
        db.close()

    def setUp(self):
        self._prepaire()
        self._current()
        self.mysql = mysql_pool
        try:
            self.dbs = []
            self.cursors = []
            for j in mysql_pool:
                self.dbs.append(MySQLdb.connect(**j))
                self.cursors.append(self.dbs[-1].cursor())
        except OperationalError, e:
            self.fail(e)

        self.db = self.dbs[self.id]
        self.cursor = self.cursors[self.id]
        self.table = mysql_table


    def tearDown(self):
        self.db.close()
