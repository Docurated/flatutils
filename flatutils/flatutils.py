import re
import os
import json
import codecs
from datetime import datetime

FIELD_INT = 1
FIELD_LONG = 2
FIELD_FLOAT = 3
FIELD_JSON = 4
FIELD_STRING = 5
FIELD_TIMESTAMP = 6
FIELD_BOOLEAN = 7

SQL_TIME_FORMAT_MS = "%Y-%m-%d %H:%M:%S.%f"
SQL_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

def _field_type_from_sql(sql_type):
    if sql_type in ['smallint', 'integer', 'smallserial', 'serial']:
        return FIELD_INT
    elif sql_type in ['bigint', 'bigserial']:
        return FIELD_LONG
    elif sql_type in ['decimal', 'numeric', 'real', 'double']:
        return FIELD_FLOAT
    elif sql_type == 'json':
        return FIELD_JSON
    elif sql_type == 'timestamp':
        return FIELD_TIMESTAMP
    elif sql_type == 'boolean':
        return FIELD_BOOLEAN
    else:
        return FIELD_STRING

def _sql_type_from_field(f):
    if f == FIELD_INT:
        return 'integer'
    elif f == FIELD_LONG:
        return 'bigint'
    elif f == FIELD_FLOAT:
        return 'double'
    elif f == FIELD_JSON:
        return 'json'
    elif f == FIELD_TIMESTAMP:
        return 'timestamp without time zone'
    elif f == FIELD_BOOLEAN:
        return 'boolean'
    else:
        return 'text'

class Field:
    def __init__(self, name, field_type, position, nullable=True):
        self.name = name
        self.field_type = field_type
        self.position = position
        self.nullable = nullable

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
