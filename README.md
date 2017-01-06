# flatutils

`flatutils` is a very simple, lightweight utility with one purpose: sort and iterate through very large pg_dumps.

Example usage:

```python
f = PgDumpFile('/opt/bigdump.sql')
sorted_f = f.output_sorted('/opt/sorteddump.sql', 'column1', 'column2')
for row in sorted_f.iterate_rows():
    print(row['column1'], row['column2']) # rows will print in order
```
