import unittest

import ir_datasets
from trectools import TrecRun

from corpus_subsampling.experiments import evaluation_on_sub_corpus

from .test_judgment_pool_corpus_sampler import DATASET_ID_FOR_TEST

RUN = TrecRun("data/unprocessed/trec-system-runs/trec13/robust/input.uogRobLT.gz")
ALL_DOCS_ROBUST04 = set()

for qrel in ir_datasets.load(DATASET_ID_FOR_TEST).qrels_iter():
    ALL_DOCS_ROBUST04.add(qrel.doc_id)


class TestRunEvaluationOnSubCorproa(unittest.TestCase):
    def test_run_evaluation_on_full_corpus(self):
        expected = {"ndcg@10": 0.4823834988625106, "skipped_qrels": 0, "unjudged@10": 0.013654618473895585}
        actual = evaluation_on_sub_corpus(RUN, ALL_DOCS_ROBUST04, DATASET_ID_FOR_TEST)

        self.assertEqual(expected, actual)

    def test_run_evaluation_10000_corpus(self):
        expected = {"ndcg@10": 0.019501021007547373, "skipped_qrels": 292862, "unjudged@10": 0.963855421686747}
        docs = set(sorted(list(ALL_DOCS_ROBUST04))[:10000])
        actual = evaluation_on_sub_corpus(RUN, docs, DATASET_ID_FOR_TEST)

        self.assertEqual(expected, actual)
