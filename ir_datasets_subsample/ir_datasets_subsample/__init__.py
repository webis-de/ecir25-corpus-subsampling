from ir_datasets import registry
from ir_datasets.datasets.base import Dataset
from ir_datasets.formats.base import BaseDocs
from ir_datasets.util import ZipExtractCache, home_path
from hashlib import md5
import os
import gzip
import json
from fastwarc import ArchiveIterator, WarcRecordType
from io import BytesIO
from resiliparse.extract.html2text import extract_plain_text
from resiliparse.parse.html import HTMLTree
from resiliparse.parse.encoding import detect_encoding

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


class WarcSubsampleDocuments(BaseDocs):
    def __init__(self, dlc):
       self.dlc = dlc
       self._docs_dict = None
    
    def __extract(self):
        return ZipExtractCache(self.dlc, home_path() / "corpus-subsamples" / self.dlc.file.name.replace('.zip', '')).path()

    def docs_iter(self):
        docs_dict = self.docs_dict()
        for k in docs_dict.keys():
            yield self.__load_doc(docs_dict[k])

    def docs_dict(self):
        if self._docs_dict is None:
            with gzip.open(self.__extract() / "documents.jsonl.gz", "rt") as f:
                docs = [json.loads(i) for i in f]
            self._docs_dict = {i["trec_id"]: i for i in docs}
        return self._docs_dict
 
    def __load_doc(self, doc):
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
                 return {
                     "docno": doc_id,
                     "url": url,
                     "main_content": extract_plain_text(html_tree, main_content=True),
                     "title": html_tree.title
                 }
             
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
   def __init__(self, zip_name, expected_md5):
       docs = WarcSubsampleDocuments(ManualZipDownload(zip_name, expected_md5))
       super().__init__(docs)

def register_subsamples():
    if f"{NAME}/clueweb12" in registry:
        return

    registry.register(f"{NAME}/clueweb09", SubsampledCorpus("clueweb09-trec-top-100.zip", "c3a1a356a875d00c172c6674cd42020b"))
    registry.register(f"{NAME}/clueweb12", SubsampledCorpus("clueweb12-trec-top-100.zip", "dfc008d0f46cb85a82d8f7a93963eaf0"))

