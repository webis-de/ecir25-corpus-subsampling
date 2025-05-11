import unittest

from ir_datasets_subsample import register_subsamples
import ir_datasets

register_subsamples()

class TestFastClueWeb09(unittest.TestCase):
    def test_2009_trec_document_can_be_loaded(self):
        expected = """Fixed Annuities - Advantage One Insurance alt=\nHome | Contact | Quote"""
        dataset = ir_datasets.load("corpus-subsamples/clueweb09/en/trec-web-2009")
        actual = dataset.docs_store().get("clueweb09-en0129-26-39854")

        self.assertEqual("Fixed Annuities - Advantage One Insurance", actual.title)
        self.assertEqual("http://www.advantageoneinsurance.com/fixed_annuities/types-annuities.php", actual.url)
        self.assertIn(expected, actual.default_text())
        self.assertTrue(actual.default_text().startswith(expected))

    def test_2010_trec_document_can_be_loaded(self):
        expected = """The concept of Being in philosophy and linguistics Home\n\nSite Map\n\nSchool of Athens\n\nOntology. A Resource Guide for Philosophers"""
        dataset = ir_datasets.load("corpus-subsamples/clueweb09/en/trec-web-2010")
        actual = dataset.docs_store().get("clueweb09-en0009-52-30967")

        self.assertEqual("The concept of Being in philosophy and linguistics", actual.title)
        self.assertEqual("http://www.formalontology.it/being.htm", actual.url)
        self.assertIn(expected, actual.default_text())
        self.assertTrue(actual.default_text().startswith(expected))

    def test_2011_trec_document_can_be_loaded(self):
        expected = 'Colorado Training and Workshops Skip Navigation.\n\nNARA\n\n  • Archives.gov Home\n  • Subject Index\n  • Contact Us\n  • FAQs\n'
        dataset = ir_datasets.load("corpus-subsamples/clueweb09/en/trec-web-2011")
        actual = dataset.docs_store().get("clueweb09-en0005-96-10047")

        self.assertEqual("Colorado Training and Workshops", actual.title)
        self.assertEqual("http://www.archives.gov/rocky-mountain/news/workshops.html", actual.url)
        self.assertIn(expected, actual.default_text())
        self.assertTrue(actual.default_text().startswith(expected))

    def test_2012_trec_document_can_be_loaded(self):
        expected = 'Category:Birds of Honduras - Wikipedia, the free encyclopedia Category:Birds of Honduras\n\nFrom Wikipedia, the free encyclopedia\n\nJump to: navigation, search\nSister project Wikimedia Commons has media related to: Birds of Honduras\n\nPages in category "Birds of Honduras"\n\nThe following 108 pages are in this category, out of 108 total. This list may sometimes be slightly out of date (learn more)\n\n  • List of birds of Honduras\n\nA\n\n  • White-flanked Antwren'
        dataset = ir_datasets.load("corpus-subsamples/clueweb09/en/trec-web-2012")
        actual = dataset.docs_store().get("clueweb09-enwp00-41-00106")

        self.assertEqual("Category:Birds of Honduras - Wikipedia, the free encyclopedia", actual.title)
        self.assertEqual("http://en.wikipedia.org/wiki/Category:Birds_of_Honduras", actual.url)
        self.assertIn(expected, actual.default_text())
        self.assertTrue(actual.default_text().startswith(expected))

    def test_2009_trec_queries_can_be_loaded(self):
        dataset = ir_datasets.load("corpus-subsamples/clueweb09/en/trec-web-2009")
        actual = None
        for i in dataset.queries_iter():
            actual = i
            break
        
        self.assertEqual("obama family tree", actual.default_text())
        self.assertEqual("1", actual.query_id)

    def test_2010_trec_queries_can_be_loaded(self):
        dataset = ir_datasets.load("corpus-subsamples/clueweb09/en/trec-web-2010")
        actual = None
        for i in dataset.queries_iter():
            actual = i
            break
        
        self.assertEqual("horse hooves", actual.default_text())
        self.assertEqual("51", actual.query_id)

    def test_2011_trec_queries_can_be_loaded(self):
        dataset = ir_datasets.load("corpus-subsamples/clueweb09/en/trec-web-2011")
        actual = None
        for i in dataset.queries_iter():
            actual = i
            break
        
        self.assertEqual("ritz carlton lake las vegas", actual.default_text())
        self.assertEqual("101", actual.query_id)

    def test_2012_trec_queries_can_be_loaded(self):
        dataset = ir_datasets.load("corpus-subsamples/clueweb09/en/trec-web-2012")
        actual = None
        for i in dataset.queries_iter():
            actual = i
            break
        
        self.assertEqual("403b", actual.default_text())
        self.assertEqual("151", actual.query_id)


    def test_2009_trec_qrels_can_be_loaded(self):
        dataset = ir_datasets.load("corpus-subsamples/clueweb09/en/trec-web-2009")
        actual = None
        for i in dataset.qrels_iter():
            actual = i
            break

        self.assertEqual("1", actual.query_id)

    def test_2010_trec_qrels_can_be_loaded(self):
        dataset = ir_datasets.load("corpus-subsamples/clueweb09/en/trec-web-2010")
        actual = None
        for i in dataset.qrels_iter():
            actual = i
            break

        self.assertEqual("51", actual.query_id)

    def test_2011_trec_qrels_can_be_loaded(self):
        dataset = ir_datasets.load("corpus-subsamples/clueweb09/en/trec-web-2011")
        actual = None
        for i in dataset.qrels_iter():
            actual = i
            break

        self.assertEqual("101", actual.query_id)

    def test_2012_trec_qrels_can_be_loaded(self):
        dataset = ir_datasets.load("corpus-subsamples/clueweb09/en/trec-web-2012")
        actual = None
        for i in dataset.qrels_iter():
            actual = i
            break

        self.assertEqual("151", actual.query_id)
