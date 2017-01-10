import re
import os
import json
from . import extsort
from datetime import datetime

FIELD_INT = 1
FIELD_FLOAT = 2
FIELD_JSON = 3
FIELD_STRING = 4
FIELD_TIMESTAMP = 5

SQL_TIME_FORMAT_MS = "%Y-%m-%d %H:%M:%S.%f"
SQL_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

def _field_type_from_sql(sql_type):
    if sql_type in ['smallint', 'integer', 'bigint',
                    'smallserial', 'serial', 'bigserial']:
        return FIELD_INT
    elif sql_type in ['decimal', 'numeric', 'real', 'double']:
        return FIELD_FLOAT
    elif sql_type == 'json':
        return FIELD_JSON
    elif sql_type == 'timestamp':
        return FIELD_TIMESTAMP
    else:
        return FIELD_STRING

class Field:
    def __init__(self, name, field_type, position):
        self.name = name
        self.field_type = field_type
        self.position = position

    def parse_value(self, value, parse_jsonb=True):
        if value == "\\N":
            return None
        elif self.field_type == FIELD_INT:
            return int(value)
        elif self.field_type == FIELD_FLOAT:
            return float(value)
        elif parse_jsonb and self.field_type == FIELD_JSON:
            return json.loads(value)
        elif self.field_type == FIELD_TIMESTAMP:
            return datetime.strptime(value, SQL_TIME_FORMAT_MS)
        else:
            return value

class Schema:
    def __init__(self, fields):
        self.fields = fields
        self.field_map = dict((f.name, f) for f in fields)

    def row_for_line(self, line, parse_jsonb=True):
        obj = {}
        values = line.strip().split("\t")
        for i, field in enumerate(self.fields):
            value = field.parse_value(values[i], parse_jsonb)
            if value is not None:
                obj[field.name] = value
        return obj

    def create_comparable(self, *field_names):
        fields = [self.field_map[f] for f in field_names]
        def comparable(line):
            values = line.strip().split("\t")
            return tuple(f.parse_value(values[f.position]) for f in fields)
        return comparable

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
        comparable = self.schema.create_comparable(*columns)
        self.copy_row_lines(output_fn)
        extsort.extsort(output_fn, comparable, temp_dir=temp_dir)
        return FlatFile(output_fn, self.schema)

    def select(self, *field_names):
        fields = [self.field_map[f] for f in field_names]
        for line in self.iterate_row_lines():
            values = line.strip().split("\t")
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
                values = line.strip().split("\t")
                key = tuple(f.parse_value(values[f.position]) for f in fields)
                if key not in output_files:
                    output_files[key] = open(
                        os.path.join(output_dir, fn_template.format(*key)), 'w')
                output_files[key].write(line)
        finally:
            for f in output_files.values():
                f.close()

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
                        line = line.replace(",", "")
                        pieces = re.split(r'\s+', line.strip())
                        fields.append(Field(
                            pieces[0], _field_type_from_sql(pieces[1]),
                            len(fields)))
                if line.startswith("CREATE TABLE"):
                    in_create = True
        return fields
