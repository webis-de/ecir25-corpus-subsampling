import unittest

from corpus_subsampling.sampling import CompleteCorpusSampler

from .test_judgment_pool_corpus_sampler import (
    DATASET_ID_FOR_TEST,
    RUN_WITH_NO_OVERLAPPING_DOCUMENTS,
    RUN_WITH_OVERLAPPING_DOCUMENTS,
)


class TestCompleteCorpusSampler(unittest.TestCase):
    def test_with_empty_runs_on_run_without_overlapping_documents(self):
        expected = set(["does-not-exist"])
        sampler = CompleteCorpusSampler({'a': RUN_WITH_NO_OVERLAPPING_DOCUMENTS})

        actual = sampler.sample_corpus(DATASET_ID_FOR_TEST, [])

        self.assertEqual(expected, actual)

    def test_with_empty_runs_on_run_with_overlapping_documents(self):
        expected = set(["FBIS4-57944", "FR940413-2-00131", "LA011890-0177"])
        sampler = CompleteCorpusSampler({'a': RUN_WITH_OVERLAPPING_DOCUMENTS})

        actual = sampler.sample_corpus(DATASET_ID_FOR_TEST, [])

        self.assertEqual(expected, actual)

    def test_with_multiple_runs(self):
        expected = set(["FBIS4-57944", "FR940413-2-00131", "LA011890-0177", "does-not-exist"])
        sampler = CompleteCorpusSampler({'a': RUN_WITH_OVERLAPPING_DOCUMENTS, 'b': RUN_WITH_NO_OVERLAPPING_DOCUMENTS})

        actual = sampler.sample_corpus(DATASET_ID_FOR_TEST, [])

        self.assertEqual(expected, actual)
