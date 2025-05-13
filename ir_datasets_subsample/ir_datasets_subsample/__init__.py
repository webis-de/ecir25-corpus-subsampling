from ir_datasets import registry
from ir_datasets.datasets.base import Dataset
from ir_datasets.formats import TrecQrel
from ir_datasets.formats.base import BaseDocs
from ir_datasets.util import ZipExtractCache, home_path, Cache
from ir_datasets.datasets.clueweb09 import TrecWebTrackQuery
from ir_datasets.datasets.clueweb12 import MisinfoQuery
from ir_datasets.util.download import RequestsDownload
from hashlib import md5
import os
import gzip
import json
from fastwarc import ArchiveIterator, WarcRecordType
from io import BytesIO
from resiliparse.extract.html2text import extract_plain_text
from resiliparse.parse.html import HTMLTree
from resiliparse.parse.encoding import detect_encoding
from typing import NamedTuple
from pathlib import Path
from ir_datasets.indices import PickleLz4FullStore
import ir_datasets
from functools import cache

NAME = "corpus-subsamples"

class ManualZipDownload():
    def __init__(self, zip_name, expected_md5):
       self.file = home_path() / "corpus-subsamples" / zip_name
       self.expected_md5 = expected_md5

    def path(self):
        if not os.path.exists(self.file):
            msg = f"Please download {self.file.name} and place it in {self.file}"
            print(msg)
            raise ValueError(msg)
        with open(self.file, 'rb') as f:
            md5_actual = str(md5(f.read()).hexdigest())
        if md5_actual != self.expected_md5:
            raise ValueError(f"The md5sum of {self.file} is wrong. Expected '{self.expected_md5}'. But got '{md5_actual}'.")
        print(f"MD5 of {self.file.name} is as expected {self.expected_md5}.")
        return self.file



class ClueWebWarcDoc(NamedTuple):
    doc_id: str
    title: str
    url: str
    text: str
    main_content: str

    def default_text(self):
        return self.title + " " + self.text

def cached_zip_resource(url, md5):
    streamer = RequestsDownload(url)
    zipped = Cache(streamer, home_path() / NAME / url.split("/")[-1])
    return ZipExtractCache(zipped, home_path() / NAME / (url.split("/")[-1] + '-extracted'))

TREC_TO_IRDS = {
    "trec-18-web-subsample.json": "clueweb09/en/trec-web-2009",
    "trec-19-web-subsample.json": "clueweb09/en/trec-web-2010",
    "trec-20-web-subsample.json": "clueweb09/en/trec-web-2011",
    "trec-21-web-subsample.json": "clueweb09/en/trec-web-2012",
    "trec-22-web-subsample.json": "clueweb12/trec-web-2013",
    "trec-23-web-subsample.json": "clueweb12/trec-web-2014",
    "trec-28-misinfo-subsample.json": "clueweb12/b13/trec-misinfo-2019",
}

class ClueWebPickleDocsstore(PickleLz4FullStore):
    def __init__(self, dlc, docs_iter_raw, allow_list):
        super().__init__(
            path=str(home_path() / "corpus-subsamples" / dlc.file.name.replace('.zip', '')) + '.pklz4',
            init_iter_fn=docs_iter_raw,
            data_cls=ClueWebWarcDoc,
            lookup_field="doc_id",
            index_fields=("doc_id",)
        )
        self.allow_list = allow_list

    def __iter__(self):
        ret = super().__iter__()
        
        if self.allow_list is not None:
            return (i for i in ret if i.doc_id in self.allow_list)
        else:
            return ret

class WarcSubsampleDocuments(BaseDocs):
    def __init__(self, dlc, trec=None):
       self.dlc = dlc
       self._docs_dict = None
       self.trec = trec
       self.docs_store_processed = None
    
    def __extract(self):
        return ZipExtractCache(self.dlc, home_path() / "corpus-subsamples" / self.dlc.file.name.replace('.zip', '')).path() / self.dlc.file.name.replace('.zip', '')

    @ir_datasets.util.use_docstore
    def docs_iter(self):
        docs_dict = self.docs_dict()
        for k in docs_dict.keys():
            yield self._load_doc(k)

        return self.docs_store().get(doc_id)

    @cache
    def allow_list(self):
         if self.trec is not None:
                topics_dir = cached_zip_resource('https://github.com/webis-de/ecir25-corpus-subsampling/raw/refs/heads/main/corpus_subsampling/warc_filtering/data/clueweb-subsamples-doc-ids.zip', '0b0d6c0c5168adffa20ffe0671d5ae40').path()
                return set(json.loads((Path(topics_dir) / self.trec).read_text()))

    def docs_dict(self):
        if self._docs_dict is None:
            allow_list = self.allow_list()
            docs = self.load_docs_dict()
            self._docs_dict = {i["trec_id"]: i for i in docs if not allow_list or i["trec_id"] in allow_list}
        return self._docs_dict
 
    def load_docs_dict(self):
        with gzip.open(self.__extract() / "document-offsets.jsonl.gz", "rt") as f:
            return [json.loads(i) for i in f]

    def __process_doc_from_warc(self, doc):
        with open(self.__extract() / doc["file"], "rb") as f:
             f.seek(doc["start_offset"])
             content = f.read(doc["end_offset"] - doc["start_offset"])
             actual_md5sum = str(md5(content).hexdigest())
             assert actual_md5sum == doc["md5"], f"Error for document {doc['trec_id']}. I expected an md5 of {doc['md5']} but got {actual_md5sum}."
             for document in ArchiveIterator(
                BytesIO(content),
                record_types=WarcRecordType.response,
                strict_mode=False
             ):
                 html_bytes = document.reader.read()
                 doc_id = document.headers["WARC-TREC-ID"]
                 url = document.headers['WARC-Target-URI']
                 assert doc_id == doc["trec_id"]
                 html_tree = HTMLTree.parse_from_bytes(html_bytes, detect_encoding(html_bytes))
                 return ClueWebWarcDoc(doc_id, html_tree.title, url, extract_plain_text(html_tree, main_content=False), extract_plain_text(html_tree, main_content=True))
             
        raise ValueError("This should not happen")       

    def docs_cls(self):
        return ClueWebWarcDoc

    def queries_cls(self):
        return TrecWebTrackQuery if "misinfo" not in self.trec else MisinfoQuery

    def qrels_cls(self):
        return TrecQrel

    def docs_store(self, field='doc_id'):
        if self.docs_store_processed is None:
            
            def docs_iter_raw():
                docs_dict = self.load_docs_dict()
                for k in docs_dict:
                    yield self.__process_doc_from_warc(k)

            self.docs_store_processed = ClueWebPickleDocsstore(self.dlc, docs_iter_raw, self.allow_list())

        return self.docs_store_processed

    def docs_namespace(self):
        raise ValueError("ToDo: Implement this")

    def docs_count(self):
        return len(self.docs_dict())

    def docs_lang(self):
        raise "en"

    def queries_iter(self):
        if not self.has_queries():
            raise ValueError("No queries available")
        ds = ir_datasets.load(TREC_TO_IRDS[self.trec])
        for i in ds.queries_iter():
            yield i

    def has_queries(self):
        return self.trec and self.trec in TREC_TO_IRDS

    def qrels_iter(self):
        if not self.has_queries():
            raise ValueError("No qrels available")
        ds = ir_datasets.load(TREC_TO_IRDS[self.trec])
        for i in ds.qrels_iter():
            yield i

class SubsampledCorpus(Dataset):
    def __init__(self, zip_name, expected_md5, trec = None):
        docs = WarcSubsampleDocuments(ManualZipDownload(zip_name, expected_md5), trec)
        self.trec = trec
        super().__init__(docs)
    
    def has_queries(self):
        return True

    def __getattr__(self, attr):
        if "queries_handler" in attr or "qrels_handler" in attr:
            return self.trec is not None and (self.trec in TREC_TO_IRDS)
        else:
            return super().__getattr__(attr)

def register_subsamples():
    if f"{NAME}/clueweb12" in registry:
        return

    registry.register(f"{NAME}/clueweb09", SubsampledCorpus("clueweb09-subcorpus.zip", "679c274cd18a84ac86bd418d9dd15e49"))
    registry.register(f"{NAME}/clueweb09/en/trec-web-2009", SubsampledCorpus("clueweb09-subcorpus.zip", "679c274cd18a84ac86bd418d9dd15e49", "trec-18-web-subsample.json"))
    registry.register(f"{NAME}/clueweb09/en/trec-web-2010", SubsampledCorpus("clueweb09-subcorpus.zip", "679c274cd18a84ac86bd418d9dd15e49", "trec-19-web-subsample.json"))
    registry.register(f"{NAME}/clueweb09/en/trec-web-2011", SubsampledCorpus("clueweb09-subcorpus.zip", "679c274cd18a84ac86bd418d9dd15e49", "trec-20-web-subsample.json"))
    registry.register(f"{NAME}/clueweb09/en/trec-web-2012", SubsampledCorpus("clueweb09-subcorpus.zip", "679c274cd18a84ac86bd418d9dd15e49", "trec-21-web-subsample.json"))
    
    registry.register(f"{NAME}/clueweb12", SubsampledCorpus("clueweb12-subcorpus.zip", "5726883a8922a518b4b412bf97951257"))
    registry.register(f"{NAME}/clueweb12/trec-web-2013", SubsampledCorpus("clueweb12-subcorpus.zip", "5726883a8922a518b4b412bf97951257", "trec-22-web-subsample.json"))
    registry.register(f"{NAME}/clueweb12/trec-web-2014", SubsampledCorpus("clueweb12-subcorpus.zip", "5726883a8922a518b4b412bf97951257", "trec-23-web-subsample.json"))
    registry.register(f"{NAME}/clueweb12/b13/trec-misinfo-2019", SubsampledCorpus("clueweb12-subcorpus.zip", "5726883a8922a518b4b412bf97951257", "trec-28-misinfo-subsample.json"))

