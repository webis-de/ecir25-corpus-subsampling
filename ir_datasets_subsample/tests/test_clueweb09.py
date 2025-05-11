import unittest

from ir_datasets_subsample import register_subsamples
import ir_datasets

register_subsamples()

WEB_TRACK_2009_DOC = "clueweb09-en0082-13-40349"
WEB_TRACK_2010_DOC = "clueweb09-en0000-78-17701"
WEB_TRACK_2011_DOC = "clueweb09-enwp01-60-05815"
WEB_TRACK_2012_DOC = "clueweb09-en0009-26-13883"

class TestClueWeb09(unittest.TestCase):
    def test_full_dataset_can_be_loaded(self):
        dataset = ir_datasets.load("corpus-subsamples/clueweb09")
        doc_ids = set()
        for i in dataset.docs_iter():
            self.assertTrue(i.doc_id not in doc_ids)
            self.assertIn("clueweb09", i.doc_id)
            doc_ids.add(i.doc_id)
        self.assertEqual(313675, len(doc_ids))
        self.assertIn(WEB_TRACK_2009_DOC, doc_ids)
        self.assertIn(WEB_TRACK_2010_DOC, doc_ids)
        self.assertIn(WEB_TRACK_2011_DOC, doc_ids)
        self.assertIn(WEB_TRACK_2012_DOC, doc_ids)

    def test_2009_trec_dataset_can_be_loaded(self):
        dataset = ir_datasets.load("corpus-subsamples/clueweb09/en/trec-web-2009")
        doc_ids = set()
        for i in dataset.docs_iter():
            self.assertTrue(i.doc_id not in doc_ids)
            self.assertIn("clueweb09", i.doc_id)
            doc_ids.add(i.doc_id)
        self.assertEqual(109657, len(doc_ids))
        self.assertIn(WEB_TRACK_2009_DOC, doc_ids)
        self.assertNotIn(WEB_TRACK_2010_DOC, doc_ids)
        self.assertNotIn(WEB_TRACK_2011_DOC, doc_ids)
        self.assertNotIn(WEB_TRACK_2012_DOC, doc_ids)

    def test_2010_trec_dataset_can_be_loaded(self):
        dataset = ir_datasets.load("corpus-subsamples/clueweb09/en/trec-web-2010")
        doc_ids = set()
        for i in dataset.docs_iter():
            self.assertTrue(i.doc_id not in doc_ids)
            self.assertIn("clueweb09", i.doc_id)
            doc_ids.add(i.doc_id)
        self.assertEqual(91047, len(doc_ids))
        self.assertNotIn(WEB_TRACK_2009_DOC, doc_ids)
        self.assertIn(WEB_TRACK_2010_DOC, doc_ids)
        self.assertNotIn(WEB_TRACK_2011_DOC, doc_ids)
        self.assertNotIn(WEB_TRACK_2012_DOC, doc_ids)

    def test_2011_trec_dataset_can_be_loaded(self):
        dataset = ir_datasets.load("corpus-subsamples/clueweb09/en/trec-web-2011")
        doc_ids = set()
        for i in dataset.docs_iter():
            self.assertTrue(i.doc_id not in doc_ids)
            self.assertIn("clueweb09", i.doc_id)
            doc_ids.add(i.doc_id)
        self.assertEqual(61851, len(doc_ids))
        self.assertNotIn(WEB_TRACK_2009_DOC, doc_ids)
        self.assertNotIn(WEB_TRACK_2010_DOC, doc_ids)
        self.assertIn(WEB_TRACK_2011_DOC, doc_ids)
        self.assertNotIn(WEB_TRACK_2012_DOC, doc_ids)

    def test_2012_trec_dataset_can_be_loaded(self):
        dataset = ir_datasets.load("corpus-subsamples/clueweb09/en/trec-web-2012")
        doc_ids = set()
        for i in dataset.docs_iter():
            self.assertTrue(i.doc_id not in doc_ids)
            self.assertIn("clueweb09", i.doc_id)
            doc_ids.add(i.doc_id)
        self.assertEqual(52501, len(doc_ids))
        self.assertNotIn(WEB_TRACK_2009_DOC, doc_ids)
        self.assertNotIn(WEB_TRACK_2010_DOC, doc_ids)
        self.assertNotIn(WEB_TRACK_2011_DOC, doc_ids)
        self.assertIn(WEB_TRACK_2012_DOC, doc_ids)

