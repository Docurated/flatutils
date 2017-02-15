import re
import csv
import os
import json
import codecs
import pandas as pd
import numpy as np
from datetime import datetime

FIELD_INT = 1
FIELD_LONG = 2
FIELD_FLOAT = 3
FIELD_JSON = 4
FIELD_STRING = 5
FIELD_TIMESTAMP = 6
FIELD_BOOLEAN = 7
FIELD_DECIMAL = 8

SQL_TIME_FORMAT_MS = "%Y-%m-%d %H:%M:%S.%f"
SQL_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

def _make_sql_to_field_mappings(d):
    m = {}
    for k, v in d.items():
        for t in v[0]:
            m[t] = k
    return m

FIELD_MAPPINGS = {
    FIELD_INT: (['integer', 'smallint', 'smallserial', 'serial'], np.int32),
    FIELD_LONG: (['bigint', 'bigserial'], np.int64),
    FIELD_DECIMAL: (['decimal', 'numeric'], np.float64),
    FIELD_FLOAT: (['real', 'double'], np.float64),
    FIELD_JSON: (['json', 'jsonb'], object),
    FIELD_TIMESTAMP: (['timestamp'], object, 'timestamp without time zone'),
    FIELD_BOOLEAN: (['boolean'], np.int8),
    FIELD_STRING: (['text'], object)
}

SQL_TO_FIELD_MAPPINGS = _make_sql_to_field_mappings(FIELD_MAPPINGS)

def _pd_type_from_field_type(f):
    return FIELD_MAPPINGS.get(f, (None, object))[1]

def _field_type_from_sql(sql_type):
    return SQL_TO_FIELD_MAPPINGS.get(sql_type, FIELD_STRING)

def _sql_type_from_field(f):
    val = FIELD_MAPPINGS.get(f, (['text'], object))
    if len(val) == 3:
        return val[2]
    else:
        return val[0][0]

class Field:
    def __init__(self, name, field_type, position, nullable=True):
        self.name = name
        self.field_type = field_type
        self.position = position
        self.nullable = nullable

    def parse_value(self, value, parse_jsonb=True):
        if value == "\\N":
            return None
        elif self.field_type == FIELD_INT or self.field_type == FIELD_LONG:
            return int(value)
        elif self.field_type == FIELD_FLOAT:
            return float(value)
        elif parse_jsonb and self.field_type == FIELD_JSON:
            return json.loads(value)
        elif self.field_type == FIELD_TIMESTAMP:
            if "." in value:
                return datetime.strptime(value, SQL_TIME_FORMAT_MS)
            else:
                return datetime.strptime(value, SQL_TIME_FORMAT)
        elif self.field_type == FIELD_BOOLEAN:
            return value == 't'
        else:
            return codecs.escape_decode(
                value.encode('utf-8'))[0].decode('utf-8')

class Schema:
    def __init__(self, fields):
        self.fields = fields
        self.field_map = dict((f.name, f) for f in fields)

    def row_for_line(self, line, parse_jsonb=True):
        obj = {}
        values = line.rstrip("\n").split("\t")
        for i, field in enumerate(self.fields):
            value = field.parse_value(values[i], parse_jsonb)
            if value is not None:
                obj[field.name] = value
        return obj

    def create_comparable(self, *field_names):
        fields = [self.field_map[f] for f in field_names]
        def comparable(line):
            values = line.rstrip("\n").split("\t")
            return tuple(f.parse_value(values[f.position]) for f in fields)
        return comparable

    def to_json(self):
        return json.dumps([[f.name, f.field_type, f.position, f.nullable] for f in self.fields])

    def to_pg_cols(self):
        cols = []
        for f in self.fields:
            c_type = _sql_type_from_field(f.field_type)
            c = "%s %s" % (f.name, c_type)
            if not f.nullable:
                c += ' NOT NULL'
            cols.append(c)
        return cols

    @staticmethod
    def from_json_file(fn):
        with open(fn, 'r') as f:
            return Schema.from_json(f.read())

    @staticmethod
    def from_json(json_str):
        return Schema([Field(f[0], f[1], f[2], f[3]) for f in json.loads(json_str)])

class FlatFile:
    def __init__(self, fn, schema):
        self.fn = fn
        self.schema = schema

    def iterate_row_lines(self):
        with open(self.fn, 'r') as f:
            for line in f:
                yield line

    def copy_row_lines(self, fn):
        with open(fn, 'w') as f:
            for line in self.iterate_row_lines():
                f.write(line)

    def iterate_rows(self, parse_jsonb=True):
        for line in self.iterate_row_lines():
            yield self.schema.row_for_line(line, parse_jsonb)

    def output_sorted(self, output_fn, *columns, temp_dir=None):
        import extsort
        comparable = self.schema.create_comparable(*columns)
        self.copy_row_lines(output_fn)
        extsort.extsort(output_fn, comparable, temp_dir=temp_dir)
        return FlatFile(output_fn, self.schema)

    def select(self, *field_names):
        fields = [self.field_map[f] for f in field_names]
        for line in self.iterate_row_lines():
            values = line.rstrip("\n").split("\t")
            yield tuple(values[f.position] for f in fields)

    def select_to_file(self, file_name, *field_names):
        fields = [self.schema.field_map[n] for n in field_names]
        with open(file_name, 'r') as f:
            for values in self.select(field_names):
                f.write("\t".join(values))
        return FlatFile(file_name, Schema(fields))

    def partition_by_fields(self, field_names, output_dir, fn_template):
        if isinstance(field_names, str):
            field_names = [field_names]
        fields = [self.schema.field_map[n] for n in field_names]
        output_files = {}
        try:
            for line in self.iterate_row_lines():
                values = line.rstrip("\n").split("\t")
                key = tuple(f.parse_value(values[f.position]) for f in fields)
                if key not in output_files:
                    output_files[key] = open(
                        os.path.join(output_dir, fn_template.format(*key)), 'w')
                output_files[key].write(line)
        finally:
            for f in output_files.values():
                f.close()

    def num_header_lines(self):
        count = 0
        with open(self.fn, 'r') as f:
            for line in f:
                count += 1
                if line.startswith("COPY "):
                    break
        return count

    def to_dataframe(self):
        def int_converter(x):
            return -1 if x == '\\N' else int(x)
        def bool_converter(x):
            if x == '\\N':
                return np.nan
            return 1 if x == 't' else 0

        converters = {}
        for f in self.schema.fields:
            if f.field_type == FIELD_BOOLEAN:
                converters[f.name] = bool_converter
            elif f.field_type == FIELD_INT or f.field_type == FIELD_LONG:
                converters[f.name] = int_converter

        return pd.read_table(
            self.fn,
            header=None,
            skiprows=self.num_header_lines(),
            names=[f.name for f in self.schema.fields],
            converters=converters,
            dtype=dict((f.name, _pd_type_from_field_type(f.field_type))
                       for f in self.schema.fields),
            na_values=["\\N"])

class PgDumpFile(FlatFile):
    def __init__(self, fn):
        self.fn = fn
        self.schema = Schema(self._to_fields())

    def iterate_row_lines(self):
        with open(self.fn, 'r') as f:
            in_copy = False
            for line in f:
                if line.startswith("\\."):
                    return
                if in_copy:
                    yield line
                elif line.startswith("COPY "):
                    in_copy = True

    def _to_fields(self):
        in_create = False
        fields = []
        with open(self.fn, 'r') as f:
            for line in f:
                if in_create:
                    if line.startswith(");"):
                        return fields
                    else:
                        line = line.replace(",", "").replace('"', '')
                        pieces = re.split(r'\s+', line.strip())
                        fields.append(Field(
                            pieces[0], _field_type_from_sql(pieces[1]),
                            len(fields),
                            'not null' not in line.lower()))
                if line.startswith("CREATE TABLE"):
                    in_create = True
        return fields

def convert_to_csv(txt_fn, out_fn):
    with open(txt_fn, 'r') as inf, open(out_fn, 'w') as outf:
        out_csv = csv.writer(outf)
        for line in inf:
            row = line.rstrip().split("\t")
            new_row = []
            for field in row:
                if field == "\\N":
                    new_row.append("")
                else:
                    new_row.append(codecs.escape_decode(
                        field.encode("utf-8"))[0].decode("utf-8"))
            out_csv.writerow(new_row)

def convert_to_tab(csv_fn, out_fn):
    with open(csv_fn, 'r') as inf, open(out_fn, 'w') as outf:
        in_csv = csv.reader(inf)
        for row in in_csv:
            new_row = []
            for field in row:
                if field == "":
                    new_row.append("\\N")
                else:
                    new_row.append(field.replace("\\", "\\\\").replace("\n", "\\n").replace("\t", "\\t")) # ended up cheating
            outf.write("\t".join(new_row))
            outf.write("\n")
