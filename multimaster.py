import unittest
from generic import Test
from uuid import uuid4
from time import sleep, time


class Test1(Test):
    def test_pass(self):
        for i in xrange(5):
            u = uuid4()
            s1 = '''INSERT INTO %s (comments, write_from) VALUES ('%s', '%s')''' % (self.table, str(u), self.mysql[self.id]['host'])
            s2 = '''SELECT comments, write_from FROM %s WHERE comments = '%s' AND write_from = '%s';''' % (self.table, str(u), self.mysql[self.id]['host'])
            q1 = self.master(commit=True, query=s1)
            self.assertEqual(q1, 1)
            q2 = self.slave(query=s2)
            self.assertEqual(q2, 1)

if __name__ == '__main__':
    unittest.main()
