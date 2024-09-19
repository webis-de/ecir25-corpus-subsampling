import unittest

from corpus_subsampling.sampling import ReRankBM25CorpusSampler, ReRankCorpusSampler

from .test_judgment_pool_corpus_sampler import (
    DATASET_ID_FOR_TEST,
    RUN_WITH_NO_OVERLAPPING_DOCUMENTS,
    RUN_WITH_OVERLAPPING_DOCUMENTS,
)


class TestReRankCorpusSampler(unittest.TestCase):
    def test_with_empty_runs_on_run_without_overlapping_documents(self):
        expected = set(["does-not-exist"])
        sampler = ReRankCorpusSampler(depth=100, run=RUN_WITH_NO_OVERLAPPING_DOCUMENTS)

        actual = sampler.sample_corpus(DATASET_ID_FOR_TEST, [])

        self.assertEqual(expected, actual)

    def test_with_empty_runs_on_run_with_overlapping_documents(self):
        expected = set(["FBIS4-57944", "FR940413-2-00131", "LA011890-0177"])
        sampler = ReRankCorpusSampler(depth=100, run=RUN_WITH_OVERLAPPING_DOCUMENTS)

        actual = sampler.sample_corpus(DATASET_ID_FOR_TEST, [])

        self.assertEqual(expected, actual)

    def test_re_rank_bm25_corpus_reranker(self):
        expected = set(["FBIS4-57944", "FR940216-0-00216", "LA011890-0177"])
        sampler = ReRankBM25CorpusSampler(depth=1000)

        actual = sampler.sample_corpus(DATASET_ID_FOR_TEST, [])

        self.assertEqual(len(actual), 165165)
        for k in expected:
            self.assertTrue(k in actual)
