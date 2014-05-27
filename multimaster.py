import unittest
from generic import Test
from uuid import uuid4
from time import sleep, time


class Test1(Test):

    def test_competition(self):
        ''' Test of competition many master-servers
        '''
        for i in xrange(10):
            u = uuid4()
            s1 = '''INSERT INTO %s (uuid, write_from) VALUES ('%s', '%s')''' % (self.table, str(u), self.mysql[self.id]['host'])
            s2 = '''SELECT uuid, write_from FROM %s WHERE uuid = '%s' AND write_from = '%s';''' % (self.table, str(u), self.mysql[self.id]['host'])
            q1 = self.master(commit=True, query=s1)
            self.assertEqual(q1, 1)
            q2 = self.slave(query=s2)
            self.assertEqual(q2, 1)

    def test_conflict(self):
        ''' Test of conflict many master-servers
        '''
        for i in xrange(10):
            s1 = '''UPDATE %s (comments, write_from) VALUES ('%s', '%s')''' % (self.table, str(u), self.mysql[self.id]['host'])
            q1 = self.master(commit=True, query=s1)
            self.assertEqual(q1, 1)

if __name__ == '__main__':
    unittest.main()
