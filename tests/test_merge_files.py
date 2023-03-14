import pytest

from test_data_for_merge import LOG_A_DATA, LOG_B_DATA, MERGE_LOG_DATA
from merge_logfile import merge_file


def create_test_file(test_log_data1, test_log_data2):
    with open('test_log_a.jsonl', 'a', encoding='utf-8') as f_o:
        f_o.writelines(test_log_data1)
    with open('test_log_b.jsonl', 'a', encoding='utf-8') as f_o:
        f_o.writelines(test_log_data2)


@pytest.mark.parametrize('log_a_data, log_b_data, merge_data',
                         [(LOG_A_DATA, LOG_B_DATA, MERGE_LOG_DATA),
                          ([], LOG_B_DATA, LOG_B_DATA),
                          (LOG_A_DATA, [], LOG_A_DATA)])
def test_merge_two_files(log_a_data, log_b_data, merge_data):
    create_test_file(log_a_data, log_b_data)
    merge_file('test_log_a.jsonl', 'test_log_b.jsonl', 'test_merge.jsonl')
    with open('test_merge.jsonl', 'r', encoding='utf-8') as f_o:
        assert f_o.readlines() == merge_data
