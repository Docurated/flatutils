import heapq
import tempfile
import os
import os.path

BLOCK_SIZE = 200 * 1024 * 1024;

def tuples_from_file(input_file, comparable_from_line, block_size=BLOCK_SIZE):
    while True:
        lines = [(comparable_from_line(l), l) for l in input_file.readlines(block_size)]
        if lines == []:
            input_file.close()
            break
        for line in lines:
            yield line

def _make_iters(input_file_name, comparable_from_line, block_size, temp_dir):
    iters = []
    total_num_blocks = (os.stat(input_file_name).st_size / block_size) + 1
    iter_block_size = int(block_size / total_num_blocks)
    with open(input_file_name, 'r') as f:
        while True:
            lines = [(comparable_from_line(l), l) for l in f.readlines(block_size)]
            if lines == []:
                break
            lines.sort(key=lambda l: l[0])
            temp_file = tempfile.TemporaryFile(dir=temp_dir, mode='w+')
            for line in lines:
                temp_file.write(line[1])
            temp_file.seek(0)
            iters.append(tuples_from_file(temp_file, comparable_from_line, iter_block_size))
    return iters

def extsort(file_name, comparable_from_line, block_size=BLOCK_SIZE, temp_dir=None):
    iters = _make_iters(file_name, comparable_from_line, block_size, temp_dir)
    output_file_name = file_name + ".out"
    try:
        with open(output_file_name, 'w') as f:
            for line_tuple in heapq.merge(*iters):
                f.write(line_tuple[1])
        os.rename(output_file_name, file_name)
    finally:
        if os.path.exists(output_file_name):
            os.unlink(output_file_name)
