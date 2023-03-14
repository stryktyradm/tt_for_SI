import argparse
import json
from datetime import datetime
from queue import Queue
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


def get_lines(path_to_file: str) -> Generator:
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


def get_log_time(file_gen: Generator,
                 buffer: Queue,
                 output_file: IO) -> Tuple[str, datetime]:
    '''
    The function takes the string returned from the files generator
    and creates a datetime object based on it
    :param file_gen: File Generator Object
    :param buffer:
    :param output_file:
    :return: A tuple consisting of a string and a datetime object
    '''
    try:
        log_line = next(file_gen)
        buffer.put(log_line)
        log_time = datetime.strptime(json.loads(log_line)['timestamp'],
                                     "%Y-%m-%d %H:%M:%S")
        return log_line, log_time
    except StopIteration:
        if not buffer.empty():
            output_file.write(buffer.get())
        raise FileGeneratorIsOver(file_gen)


def merge_file(path_to_input_file1: str,
               path_to_input_file2: str,
               path_to_output_file: str) -> None:
    '''
    The function of merging two files into one
    :param path_to_input_file1:
    :param path_to_input_file2:
    :param path_to_output_file: The resulting file
    :return: None
    '''
    file_gen1 = get_lines(path_to_file=path_to_input_file1)
    file_gen2 = get_lines(path_to_file=path_to_input_file2)

    with open(path_to_output_file, 'a', encoding='utf-8') as out:

        log_buffer1 = Queue(maxsize=2)
        log_buffer2 = Queue(maxsize=2)

        try:
            log_string1, log_time1 = get_log_time(file_gen1, log_buffer1, out)
            log_string2, log_time2 = get_log_time(file_gen2, log_buffer2, out)

            while True:
                if log_time1 <= log_time2:
                    log_string1, log_time1 = get_log_time(file_gen=file_gen1,
                                                          buffer=log_buffer1,
                                                          output_file=out)
                    out.write(log_buffer1.get())
                else:
                    log_string2, log_time2 = get_log_time(file_gen=file_gen2,
                                                          buffer=log_buffer2,
                                                          output_file=out)
                    out.write(log_buffer2.get())

        except FileGeneratorIsOver as exp:
            if exp.generator is file_gen1:
                if not log_buffer2.empty():
                    out.write(log_buffer2.get())
                merge_rest_file(file_gen2, out)
            elif exp.generator is file_gen2:
                if not log_buffer1.empty():
                    out.write(log_buffer1.get())
                merge_rest_file(file_gen1, out)


def run() -> None:
    '''
    The function takes the passed file paths as command line arguments
    and starts the file merge function.
    :return: None
    '''
    args = parse_args()
    print(f'Start merging logfile: {args.input_file[0]}, {args.input_file[1]}')
    merge_file(path_to_input_file1=args.input_file[0],
               path_to_input_file2=args.input_file[1],
               path_to_output_file=args.output)
    print(f'Finished merging!, Output file: {args.output}')


if __name__ == '__main__':
    run()
