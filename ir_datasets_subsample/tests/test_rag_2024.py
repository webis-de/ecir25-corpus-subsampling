import unittest

from ir_datasets_subsample.reformatted_corpora import register_reformatted_subsamples
import ir_datasets


class TestRAG2024(unittest.TestCase):
    def test_register_subsamples_works(self):
        register_reformatted_subsamples()

    def test_dataset_can_be_loaded(self):
        register_reformatted_subsamples()
        ds = ir_datasets.load("corpus-subsamples/trec-rag-2024")
        self.assertIsNotNone(ds)

    def test_qrels(self):
        register_reformatted_subsamples()
        ds = ir_datasets.load("corpus-subsamples/trec-rag-2024")
        qrels = [i for i in ds.qrels_iter()]
        self.assertEqual(20429, len(qrels))

        self.assertEqual("2024-105741", qrels[0].query_id)
        self.assertEqual(0, qrels[0].relevance)
        self.assertEqual("msmarco_v2.1_doc_00_125364462#6_229054655", qrels[0].doc_id)

        self.assertEqual("2024-96359", qrels[-1].query_id)
        self.assertEqual(1, qrels[-1].relevance)
        self.assertEqual("msmarco_v2.1_doc_54_724887112#1_1700994504", qrels[-1].doc_id)

    def test_queries(self):
        register_reformatted_subsamples()
        ds = ir_datasets.load("corpus-subsamples/trec-rag-2024")
        queries = [i for i in ds.queries_iter()]
        self.assertEqual(89, len(queries))

        self.assertEqual("2024-145979", queries[0].query_id)
        self.assertEqual("what is vicarious trauma and how can it be coped with?", queries[0].default_text())

        self.assertEqual("2024-127266", queries[-1].query_id)
        self.assertEqual("what are some key challenges related to the recycling of e-waste?", queries[-1].default_text())


    def test_docs_iter(self):
        register_reformatted_subsamples()
        ds = ir_datasets.load("corpus-subsamples/trec-rag-2024")
        docs = [i for i in ds.docs_iter()]
        self.assertEqual(116694, len(docs))

        self.assertEqual("msmarco_v2.1_doc_52_1400719578#0_2842698387", docs[0].doc_id)
        self.assertIn("Taylor references her past relationships and heartbreaks", docs[0].default_text())

        self.assertEqual("msmarco_v2.1_doc_53_1119612308#16_2492012852", docs[-1].doc_id)
        self.assertIn("the eCommDirect purchase is in addition to what the inmate can spend in the commissary", docs[-1].default_text())
