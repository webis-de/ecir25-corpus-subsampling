import unittest

from corpus_subsampling.sampling import LoftCorpusSampler

from .test_judgment_pool_corpus_sampler import DATASET_ID_FOR_TEST


class TestReRankCorpusSampler(unittest.TestCase):
    def test_loft_sampling_robust04_size_100(self):
        sampler = LoftCorpusSampler(target_size=100)
        actual = sampler.sample_corpus(DATASET_ID_FOR_TEST, [])
        actual = sorted(list(actual))

        self.assertEqual(100, len(actual))
        self.assertEqual(["FBIS3-14539", "FBIS3-1652", "FBIS3-21569"], actual[:3])
        self.assertEqual(["LA113090-0118", "LA121490-0092", "LA121690-0080"], actual[-3:])

    def test_loft_sampling_robust04_size_1000(self):
        sampler = LoftCorpusSampler(target_size=1000)
        actual = sampler.sample_corpus(DATASET_ID_FOR_TEST, [])
        actual = sorted(list(actual))

        self.assertEqual(1000, len(actual))
        self.assertEqual(["FBIS3-10170", "FBIS3-1079", "FBIS3-11194"], actual[:3])
        self.assertEqual(["LA123090-0066", "LA123189-0136", "LA123189-0143"], actual[-3:])

    def test_loft_sampling_robust04_size_10000(self):
        sampler = LoftCorpusSampler(target_size=10000)
        actual = sampler.sample_corpus(DATASET_ID_FOR_TEST, [])
        actual = sorted(list(actual))

        self.assertEqual(10000, len(actual))
        self.assertEqual(["FBIS3-10023", "FBIS3-10025", "FBIS3-10170"], actual[:3])
        self.assertEqual(["LA123189-0136", "LA123189-0143", "LA123189-0177"], actual[-3:])
