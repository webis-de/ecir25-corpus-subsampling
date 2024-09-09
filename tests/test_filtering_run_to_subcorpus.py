import unittest

from corpus_subsampling.run_files import filter_runs

from .test_judgment_pool_corpus_sampler import RUN_WITH_OVERLAPPING_DOCUMENTS as RUN


class TestFilteringRunToSubcorpos(unittest.TestCase):
    def test_empty_sub_corpus(self):
        expected = 0
        actual = len(filter_runs(RUN, set([])).run_data)

        self.assertEqual(expected, actual)

    def test_non_empty_sub_corpus_01(self):
        expected = {"docid": "FBIS4-57944", "query": "1", "rank": 1, "score": 1}
        actual = filter_runs(RUN, set(["FBIS4-57944"])).run_data

        self.assertEqual(1, len(actual))
        actual = actual.iloc[0].to_dict()
        self.assertEqual(expected, actual)

    def test_non_empty_sub_corpus_02(self):
        expected = {"docid": "LA011890-0177", "query": "3", "rank": 1, "score": 1}
        actual = filter_runs(RUN, set(["LA011890-0177"])).run_data

        self.assertEqual(1, len(actual))
        actual = actual.iloc[0].to_dict()
        self.assertEqual(expected, actual)
