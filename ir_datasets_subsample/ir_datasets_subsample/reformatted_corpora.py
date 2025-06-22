from ir_datasets.util import ZipExtractCache, ZipExtract, GzipExtract, home_path, Cache, _DownloadConfig
from ir_datasets import registry
from ir_datasets.formats.base import BaseDocs
from ir_datasets.formats import TrecQrels, JsonlQueries, JsonlDocs, TrecQrels
from ir_datasets.datasets.base import Dataset
from pathlib import Path
import zipfile
from typing import NamedTuple

DOWNLOAD_CONTENTS = {
    "trec-rag-2024": {
        "inputs": {
            "url": "https://cloud.uni-jena.de/s/TxQjiKpZ5YdPW3k/download/inputs.zip",
            "expected_md5": "28eb3af546608b031244a812fc9b48be",
            "size_hint": 99084224,
            "cache_path": "trec-rag-2024-inputs.zip",
        },
        "truths": {
            "url": "https://cloud.uni-jena.de/s/CENMEcHwBGe9weY/download/truths.zip",
            "expected_md5": "9c18a50ca23ef5ff7028d169abd1e210",
            "size_hint": 246262,
            "cache_path": "trec-rag-2024-truths.zip",
        }
    }
}

DownloadConfig = _DownloadConfig(contents=DOWNLOAD_CONTENTS)


class JsonlDoc(NamedTuple):
    doc_id: str
    text: str

    def default_text(self):
        return self.text


class ReformattedDataset(Dataset):
    def __init__(self, dlc):
        docs = JsonlDocs(Cache(GzipExtract(ZipExtract(dlc["inputs"], 'corpus.jsonl.gz')), Path(dlc["inputs"]._cache_path.replace(".zip", "-extracted") + '-documents.jsonl')), mapping={"doc_id": "docno", "text": "text"})
        queries = JsonlQueries(Cache(ZipExtract(dlc["inputs"], f'queries.jsonl'), Path(dlc["inputs"]._cache_path.replace(".zip", "-extracted") + '-queries.jsonl')), mapping={"query_id": "qid", "text": "query"})
        qrels = TrecQrels(Cache(ZipExtract(dlc["truths"], f'qrels.txt'), Path(dlc["truths"]._cache_path.replace(".zip", "-extracted") + '-qrels.txt')), {})

        super().__init__(docs, queries, qrels)

NAME = "corpus-subsamples"

help(ZipExtractCache)

def register_reformatted_subsamples():
    if f"{NAME}/trec-rag-2024" in registry:
        return

    for corpus in DOWNLOAD_CONTENTS.keys():
        dlc = DownloadConfig.context(corpus, home_path() / NAME / corpus)
        registry.register(f"{NAME}/{corpus}", ReformattedDataset(dlc))

