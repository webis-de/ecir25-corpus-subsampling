import unittest

from ir_datasets_subsample import register_subsamples
import ir_datasets

register_subsamples()

class TestFastClueWeb12(unittest.TestCase):
    def test_2013_trec_document_can_be_loaded(self):
        expected = """Robert Cleland - Judgepedia Robert Cleland\n\nFrom Judgepedia\n\nJump to: navigation"""
        dataset = ir_datasets.load("corpus-subsamples/clueweb12/trec-web-2013")
        actual = dataset.docs_store().get("clueweb12-0003wb-61-39818")

        self.assertEqual("Robert Cleland - Judgepedia", actual.title)
        self.assertEqual("http://judgepedia.org/index.php/Robert_Cleland", actual.url)
        self.assertIn(expected, actual.default_text())
        self.assertTrue(actual.default_text().startswith(expected))

    def test_2014_trec_document_can_be_loaded(self):
        expected = """Fitness Friday: More great 20-in-20 exercises for golfers: The Instruction Blog: Golf Digest Close\nSubscribe to Golf Digest"""
        dataset = ir_datasets.load("corpus-subsamples/clueweb12/trec-web-2014")
        actual = dataset.docs_store().get("clueweb12-1800tw-15-11215")

        self.assertEqual("Fitness Friday: More great 20-in-20 exercises for golfers: The Instruction Blog: Golf Digest", actual.title)
        self.assertEqual("http://www.golfdigest.com/golf-instruction/blogs/theinstructionblog/2012/04/fitness-friday-more-great-20-i.html?utm_source=twitterfeed&utm_medium=twitter", actual.url)
        self.assertIn(expected, actual.default_text())
        self.assertTrue(actual.default_text().startswith(expected))

    def test_2019_trec_document_can_be_loaded(self):
        expected = """Physical Therapy in Portland for Neck Pain Home\nServices\nG-Trainer\nFitness Training\nPortland\nGolf Conditioning Program\nServices\nOverview\nStaff\nAbout US"""
        dataset = ir_datasets.load("corpus-subsamples/clueweb12/b13/trec-misinfo-2019")
        actual = dataset.docs_store().get("clueweb12-1511wb-05-13480")

        self.assertEqual("Physical Therapy in Portland for Neck Pain", actual.title)
        self.assertEqual("http://www.optimalresultspt.com/Injuries-Conditions/Upper-Back-and-Neck/Upper-Back-Issues/Neck-Pain/a~306/article.html", actual.url)
        self.assertIn(expected, actual.default_text())
        self.assertTrue(actual.default_text().startswith(expected))

    def test_2013_trec_queries_can_be_loaded(self):
        dataset = ir_datasets.load("corpus-subsamples/clueweb12/trec-web-2013")
        actual = None
        for i in dataset.queries_iter():
            actual = i
            break
        
        self.assertEqual("raspberry pi", actual.default_text())
        self.assertEqual("201", actual.query_id)

    def test_2014_trec_queries_can_be_loaded(self):
        dataset = ir_datasets.load("corpus-subsamples/clueweb12/trec-web-2014")
        actual = None
        for i in dataset.queries_iter():
            actual = i
            break
        
        self.assertEqual("identifying spider bites", actual.default_text())
        self.assertEqual("251", actual.query_id)

    def test_2019_trec_queries_can_be_loaded(self):
        dataset = ir_datasets.load("corpus-subsamples/clueweb12/b13/trec-misinfo-2019")
        actual = None
        for i in dataset.queries_iter():
            actual = i
            break
        
        self.assertEqual("cranberries urinary tract infections", actual.default_text())
        self.assertEqual("1", actual.query_id)


    def test_2013_trec_qrels_can_be_loaded(self):
        dataset = ir_datasets.load("corpus-subsamples/clueweb12/trec-web-2013")
        actual = None
        for i in dataset.qrels_iter():
            actual = i
            break

        self.assertEqual("201", actual.query_id)

    def test_2014_trec_qrels_can_be_loaded(self):
        dataset = ir_datasets.load("corpus-subsamples/clueweb12/trec-web-2014")
        actual = None
        for i in dataset.qrels_iter():
            actual = i
            break

        self.assertEqual("251", actual.query_id)

    def test_2019_trec_qrels_can_be_loaded(self):
        dataset = ir_datasets.load("corpus-subsamples/clueweb12/b13/trec-misinfo-2019")
        actual = None
        for i in dataset.qrels_iter():
            actual = i
            break

        self.assertEqual("1", actual.query_id)

