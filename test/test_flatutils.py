import os
import tempfile
import unittest
from extsort import extsort
import datetime

from flatutils import PgDumpFile, FlatFile
from . import DATA_DIR

class TestFlatUtils(unittest.TestCase):
    def test_iterate_pg_dump(self):
        fn = os.path.join(DATA_DIR, 'groups.sql')
        f = PgDumpFile(fn)
        record = None
        for i, row in enumerate(f.iterate_rows()):
            if i == 4:
                record = row
                break
        self.assertEqual(
            ['created_at', 'id', 'name', 'organization_id', 'source_id', 'updated_at'],
            sorted(record.keys()))
        self.assertEqual(
            datetime.datetime(2014, 8, 26, 14, 42, 13, 839069),
            record['created_at'])
        self.assertEqual(2232, record['id'])
        self.assertEqual('group72', record['name'])
        self.assertEqual(337, record['organization_id'])
        self.assertEqual('group source 72', record['source_id'])
        self.assertEqual(
            datetime.datetime(2014, 8, 26, 14, 42, 13, 839069),
            record['updated_at'])

    def test_sort_pg_dump_simple(self):
        fn = os.path.join(DATA_DIR, 'groups.sql')
        f = PgDumpFile(fn)
        outf, outfn = tempfile.mkstemp()
        os.close(outf)
        f.output_sorted(outfn, "id")
        last_id = 0
        count = 0
        with open(outfn, 'r') as outf:
            for line in outf:
                cur_id = int(line.split("\t")[0])
                if last_id >= cur_id:
                    self.fail()
                last_id = cur_id
                count += 1
        self.assertEqual(875, count)

    def test_sort_pg_dump_multicolumn(self):
        fn = os.path.join(DATA_DIR, 'groups.sql')
        f = PgDumpFile(fn)
        outf, outfn = tempfile.mkstemp()
        os.close(outf)
        f.output_sorted(outfn, "organization_id", "created_at")
        sorted_file = FlatFile(outfn, f.schema)
        prev_tuple = None
        count = 0
        for row in sorted_file.iterate_rows():
            cur_tuple = (row['organization_id'], row['created_at'])
            if prev_tuple is not None:
                if cur_tuple[0] < prev_tuple[0]:
                    self.fail()
                elif cur_tuple[0] == prev_tuple[0]:
                    if cur_tuple[1] < prev_tuple[1]:
                        self.fail()
            count += 1
        self.assertEqual(875, count)
