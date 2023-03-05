import argparse
import json
from datetime import datetime
from exception import GeneratorIsOver


def parse_args():
    parser = argparse.ArgumentParser(description='Tool to merge two logfile')

    parser.add_argument(
        'input_file',
        type=str,
        help='path to the logfile',
        nargs=2
    )

    parser.add_argument(
        '-o', '--output',
        type=str,
        required=True,
        help='path to output file'
    )

    return parser.parse_args()


def get_lines(path_to_file):
    with open(path_to_file, 'r') as log_file:
        for line in log_file:
            yield line


def merge_rest_file(filegenerator, output_file):
    for line in filegenerator:
        output_file.write(line)


def get_log_time(filegenerator):
    try:
        log_line = next(filegenerator)
        log_time = datetime.strptime(json.loads(log_line)['timestamp'], "%Y-%m-%d %H:%M:%S")
        return log_line, log_time
    except StopIteration:
        raise GeneratorIsOver(filegenerator)


def merge_file(file_gen1, file_gen2, path_to_output_file):
    out = open(path_to_output_file, 'a')

    try:

        log_string1, log_time1 = get_log_time(file_gen1)
        log_string2, log_time2 = get_log_time(file_gen2)

        while True:
            if log_time1 <= log_time2:
                out.write(log_string1)
                log_string1, log_time1 = get_log_time(file_gen1)
            else:
                out.write(log_string2)
                log_string2, log_time2 = get_log_time(file_gen2)

    except GeneratorIsOver as exp:

        if exp.generator is file_gen1:
            merge_rest_file(file_gen2, out)
        elif exp.generator is file_gen2:
            merge_rest_file(file_gen1, out)

    finally:
        out.close()


def run():
    args = parse_args()
    print(f'Start merging two logfile: {args.input_file[0]}, {args.input_file[1]}')
    log1 = get_lines(path_to_file=args.input_file[0])
    log2 = get_lines(path_to_file=args.input_file[1])
    merge_file(log1, log2, path_to_output_file=args.output)
    print(f'Finished merging!')


if __name__ == '__main__':
    run()
