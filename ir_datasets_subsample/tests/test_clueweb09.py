import unittest

from ir_datasets_subsample import register_subsamples
import ir_datasets

register_subsamples()

class TestClueWeb09(unittest.TestCase):
    def test_dataset_can_be_loaded(self):
        dataset = ir_datasets.load("corpus-subsamples/clueweb09")
        doc_ids = set()
        for i in dataset.docs_iter():
            self.assertTrue(i.doc_id not in doc_ids)
            self.assertIn("clueweb09", i.doc_id)
            doc_ids.add(i.doc_id)
        self.assertEqual(313675, len(doc_ids))
        self.assertIn("clueweb09-en0110-22-31947", doc_ids)
        self.assertIn("clueweb09-en0007-35-14341", doc_ids)

