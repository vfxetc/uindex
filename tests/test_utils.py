from . import *

from uindex.utils import *


class TestParseBytes(TestCase):
    
    def test_basics(self):

        self.assertEqual(parse_bytes('0'), 0)
        self.assertEqual(parse_bytes('1'), 1)

        self.assertEqual(parse_bytes('123'), 123)
        self.assertEqual(parse_bytes('123k'), 123 * 1024)
        self.assertEqual(parse_bytes('123M'), 123 * 1024 ** 2)
        self.assertEqual(parse_bytes('123G'), 123 * 1024 ** 3)
        self.assertEqual(parse_bytes('123T'), 123 * 1024 ** 4)
        self.assertEqual(parse_bytes('123P'), 123 * 1024 ** 5)

        self.assertEqual(parse_bytes('123B'), 123)
        self.assertEqual(parse_bytes('123kB'), 123 * 1024)
        self.assertEqual(parse_bytes('123MB'), 123 * 1024 ** 2)
        self.assertEqual(parse_bytes('123GB'), 123 * 1024 ** 3)
        self.assertEqual(parse_bytes('123TB'), 123 * 1024 ** 4)
        self.assertEqual(parse_bytes('123PB'), 123 * 1024 ** 5)

