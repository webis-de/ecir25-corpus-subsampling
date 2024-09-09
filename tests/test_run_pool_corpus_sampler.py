import unittest

from corpus_subsampling.sampling import RunPoolCorpusSampler

from .test_judgment_pool_corpus_sampler import (
    DATASET_ID_FOR_TEST,
    RUN_WITH_NO_OVERLAPPING_DOCUMENTS,
    RUN_WITH_OVERLAPPING_DOCUMENTS,
)


class TestJudgmentPoolCorpusSampler(unittest.TestCase):
    def test_with_empty_runs(self):
        expected = set()
        sampler = RunPoolCorpusSampler(depth=100)

        actual = sampler.sample_corpus(DATASET_ID_FOR_TEST, [])

        self.assertEqual(expected, actual)

    def test_with_run_without_overlapping_doc_ids(self):
        expected = set(["does-not-exist"])
        sampler = RunPoolCorpusSampler(depth=100)

        actual = sampler.sample_corpus(DATASET_ID_FOR_TEST, [RUN_WITH_NO_OVERLAPPING_DOCUMENTS])

        self.assertEqual(expected, actual)

    def test_with_run_with_overlapping_doc_ids(self):
        expected = set(["FBIS4-57944", "FR940413-2-00131", "LA011890-0177"])
        sampler = RunPoolCorpusSampler(depth=100)

        actual = sampler.sample_corpus(DATASET_ID_FOR_TEST, [RUN_WITH_OVERLAPPING_DOCUMENTS])

        self.assertEqual(expected, actual)

    def test_with_multiple_runs(self):
        expected = set(["FBIS4-57944", "FR940413-2-00131", "LA011890-0177", "does-not-exist"])
        sampler = RunPoolCorpusSampler(depth=100)

        actual = sampler.sample_corpus(
            DATASET_ID_FOR_TEST, [RUN_WITH_OVERLAPPING_DOCUMENTS, RUN_WITH_NO_OVERLAPPING_DOCUMENTS]
        )

        self.assertEqual(expected, actual)

    def test_string_representation_depth_10(self):
        expected = "top-10-run-pool"
        actual = str(RunPoolCorpusSampler(depth=10))

        self.assertEqual(expected, actual)

    def test_string_representation_depth_100(self):
        expected = "top-100-run-pool"
        actual = str(RunPoolCorpusSampler(depth=100))

        self.assertEqual(expected, actual)
