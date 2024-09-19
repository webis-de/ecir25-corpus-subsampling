import unittest

import ir_datasets

from corpus_subsampling.sampling import LoftCorpusSampler

from .test_judgment_pool_corpus_sampler import DATASET_ID_FOR_TEST

judged_relevant = set([i.doc_id for i in ir_datasets.load(DATASET_ID_FOR_TEST).qrels_iter() if i.relevance > 0])


class TestReRankCorpusSampler(unittest.TestCase):
    def test_loft_sampling_robust04_size_100(self):
        sampler = LoftCorpusSampler(target_size=100)
        actual = sampler.sample_corpus(DATASET_ID_FOR_TEST, [])
        actual = sorted(list(actual))

        self.assertEqual(100, len(actual))
        self.assertEqual(4, len([i for i in actual if i in judged_relevant]))

    def test_loft_sampling_robust04_size_1000(self):
        sampler = LoftCorpusSampler(target_size=1000)
        actual = sampler.sample_corpus(DATASET_ID_FOR_TEST, [])
        actual = sorted(list(actual))

        self.assertEqual(1000, len(actual))
        self.assertEqual(31, len([i for i in actual if i in judged_relevant]))

    def test_loft_sampling_robust04_size_10000(self):
        sampler = LoftCorpusSampler(target_size=10000)
        actual = sampler.sample_corpus(DATASET_ID_FOR_TEST, [])
        actual = sorted(list(actual))

        self.assertEqual(10000, len(actual))
        self.assertEqual(330, len([i for i in actual if i in judged_relevant]))

    def test_loft_sampling_robust04_size_25000(self):
        sampler = LoftCorpusSampler(target_size=30000)
        actual = sampler.sample_corpus(DATASET_ID_FOR_TEST, [])
        actual = sorted(list(actual))

        self.assertEqual(25000, len(actual))
        self.assertEqual(889, len([i for i in actual if i in judged_relevant]))
