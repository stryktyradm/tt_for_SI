import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Generator, Tuple, IO

from custom_exception import FileGeneratorIsOver


def parse_args() -> argparse.Namespace:
    '''
    The function collects command line arguments
    :return: argparse.Namespace
    '''
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


def get_lines(path_to_file: Path) -> Generator[str]:
    '''
    A generator that opens a file and returns it line by line
    :param path_to_file: the path to the file to be opened and read
    :return: str
    '''
    with open(path_to_file, 'r', encoding='utf-8') as log_file:
        for line in log_file:
            yield line


def merge_rest_file(filegenerator: Generator, output_file: IO) -> None:
    '''
    The function goes through the file generator passed to it
    and writes the remaining lines to the resulting file
    :param filegenerator: Generator files in which I can leave unread lines
    :param output_file: The resulting file
    :return: None
    '''
    for line in filegenerator:
        output_file.write(line)


def get_log_time(filegenerator: Generator) -> Tuple[str, datetime]:
    '''
    The function takes the string returned from the files generator
    and creates a datetime object based on it
    :param filegenerator: File Generator Object
    :return: A tuple consisting of a string and a datetime object
    '''
    try:
        log_line = next(filegenerator)
        log_time = datetime.strptime(json.loads(log_line)['timestamp'],
                                     "%Y-%m-%d %H:%M:%S")
        return log_line, log_time
    except StopIteration:
        raise FileGeneratorIsOver(filegenerator)


def merge_file(file_gen1: Generator,
               file_gen2: Generator,
               path_to_output_file: Path) -> None:
    '''
    The function of merging two files into one
    :param file_gen1: File Generator Object
    :param file_gen2: File Generator Object
    :param path_to_output_file: The resulting file
    :return: None
    '''
    with open(path_to_output_file, 'a', encoding='utf-8') as out:

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

        except FileGeneratorIsOver as exp:
            if exp.generator is file_gen1:
                merge_rest_file(file_gen2, out)
            elif exp.generator is file_gen2:
                merge_rest_file(file_gen1, out)


def run() -> None:
    '''
    The function takes the passed file paths as command line arguments
    and starts the file merge function.
    :return: None
    '''
    args = parse_args()
    print(f'Start merging logfile: {args.input_file[0]}, {args.input_file[1]}')
    log1 = get_lines(path_to_file=args.input_file[0])
    log2 = get_lines(path_to_file=args.input_file[1])
    merge_file(log1, log2, path_to_output_file=args.output)
    print(f'Finished merging!, Output file: {args.output}')


if __name__ == '__main__':
    run()
