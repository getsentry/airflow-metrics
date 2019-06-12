from unittest import TestCase


class TestPatching(TestCase):
    def test_patching(self):
        '''
        airflow-metrics automatically patches the package upon loading, so this empty test is just
        here to make a note that if the tests run and pass, the patching is working properly
        '''
        pass
