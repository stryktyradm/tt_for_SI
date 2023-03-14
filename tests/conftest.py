import pytest
import os


@pytest.fixture(scope='function', autouse=True)
def remove_test_log_file():
    yield
    for file in ['test_log_a.jsonl', 'test_log_b.jsonl', 'test_merge.jsonl']:
        path = os.path.join(os.path.abspath(os.path.dirname(__file__)), file)
        os.remove(path)
