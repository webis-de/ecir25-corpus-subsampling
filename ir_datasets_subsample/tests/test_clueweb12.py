import unittest

from ir_datasets_subsample import register_subsamples
import ir_datasets

register_subsamples()

WEB_TRACK_2013_DOC = "clueweb12-0002wb-02-35410"
WEB_TRACK_2014_DOC = "clueweb12-0300wb-79-33825"
MISINFO_TRACK_2019_DOC = "clueweb12-1807wb-94-03854"

class TestClueWeb12(unittest.TestCase):
    def test_full_dataset_can_be_loaded(self):
        dataset = ir_datasets.load("corpus-subsamples/clueweb12")
        doc_ids = set()
        for i in dataset.docs_iter():
            self.assertTrue(i.doc_id not in doc_ids)
            self.assertIn("clueweb12", i.doc_id)
            doc_ids.add(i.doc_id)
        self.assertEqual(225044, len(doc_ids))
        self.assertIn(WEB_TRACK_2013_DOC, doc_ids)
        self.assertIn(WEB_TRACK_2014_DOC, doc_ids)
        self.assertIn(MISINFO_TRACK_2019_DOC, doc_ids)

    def test_trec_13_dataset_can_be_loaded(self):
        dataset = ir_datasets.load("corpus-subsamples/clueweb12/trec-web-2013")
        doc_ids = set()
        for i in dataset.docs_iter():
            self.assertTrue(i.doc_id not in doc_ids)
            self.assertIn("clueweb12", i.doc_id)
            doc_ids.add(i.doc_id)
        self.assertEqual(87811, len(doc_ids))
        self.assertIn(WEB_TRACK_2013_DOC, doc_ids)
        self.assertNotIn(WEB_TRACK_2014_DOC, doc_ids)
        self.assertNotIn(MISINFO_TRACK_2019_DOC, doc_ids)

    def test_trec_14_dataset_can_be_loaded(self):
        dataset = ir_datasets.load("corpus-subsamples/clueweb12/trec-web-2014")
        doc_ids = set()
        for i in dataset.docs_iter():
            self.assertTrue(i.doc_id not in doc_ids)
            self.assertIn("clueweb12", i.doc_id)
            doc_ids.add(i.doc_id)
        self.assertEqual(84241, len(doc_ids))
        self.assertNotIn(WEB_TRACK_2013_DOC, doc_ids)
        self.assertIn(WEB_TRACK_2014_DOC, doc_ids)
        self.assertNotIn(MISINFO_TRACK_2019_DOC, doc_ids)


    def test_trec_misinfo_dataset_can_be_loaded(self):
        dataset = ir_datasets.load("corpus-subsamples/clueweb12/b13/trec-misinfo-2019")
        doc_ids = set()
        for i in dataset.docs_iter():
            self.assertTrue(i.doc_id not in doc_ids)
            self.assertIn("clueweb12", i.doc_id)
            doc_ids.add(i.doc_id)
        self.assertEqual(53583, len(doc_ids))
        self.assertNotIn(WEB_TRACK_2013_DOC, doc_ids)
        self.assertNotIn(WEB_TRACK_2014_DOC, doc_ids)
        self.assertIn(MISINFO_TRACK_2019_DOC, doc_ids)
