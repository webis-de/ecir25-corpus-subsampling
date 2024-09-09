from abc import ABC, abstractmethod

import ir_datasets
from trectools import TrecRun


class CorpusSampler(ABC):
    """Sample a set of documents from a large corpus."""

    @abstractmethod
    def sample_corpus(ir_datasets_id: str, runs: list[TrecRun]) -> set[str]:
        """Sample a corpus (returned as a set of document IDs)
        for a given dataset and a set of runs that were used
        to cunstruct the pool.

        Args:
            ir_datasets_id (str): The ir_datasets ID of the dataset.
            runs (list[TrecRun]): The runs used to construct the pool.

        Returns:
            set[str]: The sampled corpus as a set of document IDs.
        """
        pass


class JudgmentPoolCorpusSampler(CorpusSampler):
    def sample_corpus(self, ir_datasets_id: str, runs: list[TrecRun]) -> set[str]:
        """Sample a corpus (returned as a set of document IDs)
        by just returning all judged documents from the judgment pool.

        Args:
            ir_datasets_id (str): The ir_datasets ID of the dataset.
            runs (list[TrecRun]): The runs used to construct the pool.

        Returns:
            set[str]: The judged documents as sampled corpus.
        """
        allowed_document_ids = set()
        for run in runs:
            for doc_id in run.run_data["docid"]:
                allowed_document_ids.add(doc_id)

        qrels_iter = ir_datasets.load(ir_datasets_id).qrels_iter()
        ret = set()

        for qrel in qrels_iter:
            if qrel.doc_id in allowed_document_ids:
                ret.add(qrel.doc_id)

        return ret
