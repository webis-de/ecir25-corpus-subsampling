from ir_datasets import registry
from ir_datasets.datasets.base import Dataset
from ir_datasets.formats.base import BaseDocs
from ir_datasets.util import ZipExtractCache, home_path, Cache
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

    def default_text(self):
        return self.title + " " + self.text

def cached_zip_resource(url, md5):
    streamer = RequestsDownload(url)
    zipped = Cache(streamer, home_path() / NAME / url.split("/")[-1])
    return ZipExtractCache(zipped, home_path() / NAME / (url.split("/")[-1] + '-extracted'))


class WarcSubsampleDocuments(BaseDocs):
    def __init__(self, dlc, trec=None):
       self.dlc = dlc
       self._docs_dict = None
       self.trec = trec
    
    def __extract(self):
        return ZipExtractCache(self.dlc, home_path() / "corpus-subsamples" / self.dlc.file.name.replace('.zip', '')).path() / self.dlc.file.name.replace('.zip', '')

    def docs_iter(self):
        docs_dict = self.docs_dict()
        for k in docs_dict.keys():
            yield self.__load_doc(docs_dict[k])

    def docs_dict(self):
        if self._docs_dict is None:

            allow_list = None
            if self.trec is not None:
                topics_dir = cached_zip_resource('https://github.com/webis-de/ecir25-corpus-subsampling/raw/refs/heads/main/corpus_subsampling/warc_filtering/data/clueweb-subsamples-doc-ids.zip', '0b0d6c0c5168adffa20ffe0671d5ae40').path()
                allow_list = set(json.loads((Path(topics_dir) / self.trec).read_text()))
            with gzip.open(self.__extract() / "document-offsets.jsonl.gz", "rt") as f:
                docs = [json.loads(i) for i in f]
            self._docs_dict = {i["trec_id"]: i for i in docs if not allow_list or i["trec_id"] in allow_list}
        return self._docs_dict
 
    def __load_doc(self, doc):
        return ClueWebWarcDoc(doc["trec_id"], "title", "url", "plain_text")
        with open(self.__extract() / doc["file"], "rb") as f:
             f.seek(doc["start_offset"])
             content = f.read(doc["end_offset"] - doc["start_offset"])
             # actual_md5sum = str(md5(content).hexdigest())
             for document in ArchiveIterator(
                BytesIO(content),
                record_types=WarcRecordType.response,
                parse_http=True,
                auto_decode="all"
             ):
                 doc_id = document.headers["WARC-TREC-ID"]
                 assert doc_id == doc["trec_id"]
                 url = document.headers['WARC-Target-URI']

                 html_bytes = document.reader.read()
                 html_tree = HTMLTree.parse_from_bytes(html_bytes, detect_encoding(html_bytes))
                 return ClueWebWarcDoc(doc_id, html_tree.title, url, extract_plain_text(html_tree, main_content=True))
             
        raise ValueError("fooo")       

    def docs_cls(self):
        return self._cls

    def docs_store(self, field='doc_id'):
        raise ValueError("ToDo: Implement this")

    def docs_namespace(self):
        raise ValueError("ToDo: Implement this")

    def docs_count(self):
        return len(self.docs_dict())

    def docs_lang(self):
        raise ValueError("ToDo: Implement this")


class SubsampledCorpus(Dataset):
   def __init__(self, zip_name, expected_md5, trec = None):
       docs = WarcSubsampleDocuments(ManualZipDownload(zip_name, expected_md5), trec)
       self.trec = trec
       super().__init__(docs)

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

