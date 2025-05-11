import unittest

from ir_datasets_subsample import register_subsamples
import ir_datasets

register_subsamples()

class TestClueWeb12(unittest.TestCase):
    def test_dataset_can_be_loaded(self):
        dataset = ir_datasets.load("corpus-subsamples/clueweb12")
        doc_ids = set()
        for i in dataset.docs_iter():
            self.assertTrue(i.doc_id not in doc_ids)
            self.assertIn("clueweb12", i.doc_id)
            doc_ids.add(i.doc_id)
        self.assertEqual(313675, len(doc_ids))
        self.assertIn("clueweb12-0002wb-02-35410", doc_ids)
        self.assertIn("clueweb12-0300wb-79-33825", doc_ids)

