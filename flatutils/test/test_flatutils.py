import os
import re
import tempfile
import unittest
import numpy as np
from extsort import extsort
import datetime

from flatutils import PgDumpFile, FlatFile, Schema, Field, FIELD_INT, \
    FIELD_STRING, FIELD_LONG, FIELD_JSON, FIELD_TIMESTAMP
import flatutils
from . import DATA_DIR

def _data_file(fn):
    return os.path.join(DATA_DIR, fn)

def _file_text(fn):
    with open(fn, 'r') as f:
        return f.read()

class TestFlatUtils(unittest.TestCase):
    def test_iterate_pg_dump(self):
        fn = _data_file('groups.sql')
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

    def test_parse_escapes(self):
        fn = _data_file('groups.sql')
        f = PgDumpFile(fn)
        record = None
        for i, row in enumerate(f.iterate_rows()):
            if i == 2:
                record = row
                break
        self.assertEqual("group so\nur\tce 70", record['source_id'])

    def test_sort_pg_dump_simple(self):
        fn = _data_file('groups.sql')
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
        fn = _data_file('groups.sql')
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

    def test_partition_by_one_field(self):
        fn = _data_file('groups.sql')
        f = PgDumpFile(fn)
        counts = {}
        for row in f.iterate_rows():
            org_id = row['organization_id']
            counts[org_id] = counts.get(org_id, 0) + 1
        with tempfile.TemporaryDirectory() as dir_name:
            f.partition_by_fields("organization_id", dir_name, "out{0}.txt")
            file_names = os.listdir(dir_name)
            self.assertEqual(len(counts), len(file_names))
            for fn in file_names:
                org_id = int(re.search(r"\d+", fn).group(0))
                count = 0
                with open(os.path.join(dir_name, fn), 'r') as f:
                    count = len(f.readlines())
                self.assertEqual(counts[org_id], count)

    def test_serdeser(self):
        fn = _data_file('groups.sql')
        f = PgDumpFile(fn)
        schema_0 = f.schema
        schema_1 = Schema.from_json(schema_0.to_json())
        self.assertEqual(len(schema_0.fields), len(schema_1.fields))
        for field_0 in schema_0.fields:
            field_1 = schema_1.field_map[field_0.name]
            self.assertEqual(field_0.field_type, field_1.field_type)
            self.assertEqual(field_0.position, field_1.position)

    def test_schema(self):
        fn = _data_file('groups.sql')
        f = PgDumpFile(fn)
        schema = f.schema
        self.assertEqual(True, schema.field_map['name'].nullable)
        self.assertEqual(False, schema.field_map['created_at'].nullable)
        self.assertEqual(True, schema.field_map['organization_id'].nullable)

    def test_schema_null_ser(self):
        s = Schema([Field("test", FIELD_INT, 0, False)])
        s = Schema.from_json(s.to_json())
        self.assertEqual(s.fields[0].nullable, False)

    def test_pg_cols(self):
        fn = _data_file('groups.sql')
        f = PgDumpFile(fn)
        pg_cols = f.schema.to_pg_cols()
        self.assertEqual('id integer NOT NULL', pg_cols[0])
        self.assertEqual('name text', pg_cols[1])
        self.assertEqual('created_at timestamp without time zone NOT NULL', pg_cols[2])

    def test_convert_tab_to_csv(self):
        fn = _data_file('dump.txt')
        outf, outfn = tempfile.mkstemp()
        os.close(outf)
        try:
            flatutils.convert_to_csv(fn, outfn)
            self.assertEqual(_file_text(_data_file('dump.csv')),
                             _file_text(outfn))
        finally:
            os.unlink(outfn)

    def test_convert_csv_to_tab(self):
        fn = _data_file('dump.csv')
        outf, outfn = tempfile.mkstemp()
        os.close(outf)
        try:
            flatutils.convert_to_tab(fn, outfn)
            self.assertEqual(_file_text(_data_file('dump.txt')),
                             _file_text(outfn))
        finally:
            os.unlink(outfn)

    def test_to_dataframe(self):
        data = [
            ["5", "abc", '{"hello": "computer"}', "2016-06-06 23:12:36"],
            ["\\N", "def", '{"hello": "computer"}', "\\N"]
        ]
        wf, wfn = tempfile.mkstemp()
        os.close(wf)
        with open(wfn, "w") as f:
            for line in data:
                f.write("\t".join(line))
                f.write("\n")
        schema = Schema([
            Field("id", FIELD_LONG, 0, True),
            Field("name", FIELD_STRING, 1, False),
            Field("data", FIELD_JSON, 2, False),
            Field("time", FIELD_TIMESTAMP, 3, True)])
        ff = FlatFile(wfn, schema)
        df = ff.to_dataframe()
        self.assertEqual("id", df.columns[0])
        self.assertEqual("name", df.columns[1])
        self.assertEqual("data", df.columns[2])
        self.assertEqual("time", df.columns[3])
        self.assertEqual(5.0, df["id"].iloc[0])
        self.assertTrue(np.isnan(df["id"].iloc[1]))
