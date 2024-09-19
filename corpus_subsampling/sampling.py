import gzip
import json
from abc import ABC, abstractmethod
from pathlib import Path

import ir_datasets
from trectools import TrecPoolMaker, TrecRun


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

    def __str__(self) -> str:
        return "judgment-pool"


class CompleteCorpusSampler(CorpusSampler):
    def __init__(self, runs):
        self.corpus = set()
        for run in runs.values():
            for doc_id in run.run_data["docid"]:
                self.corpus.add(doc_id)

    def sample_corpus(self, ir_datasets_id: str, runs: list[TrecRun]) -> set[str]:
        return self.corpus

    def __str__(self) -> str:
        return "complete-corpus"


class RunPoolCorpusSampler(CorpusSampler):
    def __init__(self, depth: int):
        """Create a pool of the passed depth for the passed runs as sampled corpus.

        Args:
            depth (int): The depth of the pool
        """
        self.depth = depth

    def sample_corpus(self, ir_datasets_id: str, runs: list[TrecRun]) -> set[str]:
        """Sample a corpus (returned as a set of document IDs)
        by returning the top-k pool of all runs that formed
        the original judgment pool.

        Args:
            ir_datasets_id (str): The ir_datasets ID of the dataset.
            runs (list[TrecRun]): The runs used to construct the pool.

        Returns:
            set[str]: The top-k pool of the runs as sampled corpus
        """
        ret = set()
        pool = TrecPoolMaker().make_pool(runs, strategy="topX", topX=self.depth).pool

        for docids in pool.values():
            ret.update(docids)

        return ret

    def __str__(self) -> str:
        return f"top-{self.depth}-run-pool"


class ReRankCorpusSampler(RunPoolCorpusSampler):
    def __init__(self, depth: int, run: TrecRun):
        """Create a corpus that just re-ranks an existing first-stage retriever.
        I.e., this basically builds a pool of the of the passed depth for the passed run as sampled corpus.

        Args:
            depth (int): The depth of the pool
        """
        super().__init__(depth)
        self.run = run

    def sample_corpus(self, ir_datasets_id: str, runs: list[TrecRun]) -> set[str]:
        """Sample a corpus (returned as a set of document IDs)
        by just re-ranking the baseline run.

        Args:
            ir_datasets_id (str): The ir_datasets ID of the dataset.
            runs (list[TrecRun]): The runs used to construct the pool,
                                  not used here, as just the initial basline is re-ranked.

        Returns:
            set[str]: The corpus that just re-ranks an initial baseline run.
        """
        return super().sample_corpus(ir_datasets_id, [self.run])


class ReRankBM25CorpusSampler(ReRankCorpusSampler):
    def __init__(self, depth: int):
        from tira.rest_api_client import Client

        self.tira = Client()
        self.depth = depth

    def sample_corpus(self, ir_datasets_id: str, runs: list[TrecRun]) -> set[str]:
        from tira.tirex import IRDS_TO_TIREX_DATASET

        run = (
            Path(
                self.tira.get_run_output(
                    "ir-benchmarks/tira-ir-starter/BM25 Re-Rank (tira-ir-starter-pyterrier)",
                    IRDS_TO_TIREX_DATASET[ir_datasets_id],
                )
            )
            / "run.txt"
        )
        self.run = TrecRun(run)

        return super().sample_corpus(ir_datasets_id, runs)

    def __str__(self) -> str:
        return f"re-rank-top-{self.depth}-bm25"


class LoftCorpusSampler(CorpusSampler):
    def __init__(self, target_size: int, random_documents: set[str] = None):
        self.target_size = target_size
        self.__random_documents = random_documents
        self.depth = 10

    def random_documents(self, ir_dataset_id: str):
        if self.__random_documents:
            return self.__random_documents.copy()

        default_path = (
            Path(__file__).parent.parent.resolve()
            / "data"
            / "processed"
            / "random-documents"
            / (ir_dataset_id.split("/")[0] + ".json.gz")
        )

        with gzip.open(default_path, "rt") as f:
            return json.load(f)

    def pool(self, runs: list[TrecRun]) -> set[str]:
        ret = set()
        pool = TrecPoolMaker().make_pool(runs, strategy="topX", topX=self.depth).pool

        for docids in pool.values():
            ret.update(docids)

        return ret

    def sample_corpus(self, ir_datasets_id: str, runs: list[TrecRun]) -> set[str]:
        dataset = ir_datasets.load(ir_datasets_id)
        complete_pool = self.pool(runs)

        qid_to_relevant_documents = {}
        for qrel in dataset.qrels_iter():
            if qrel.relevance <= 0 or qrel.doc_id not in complete_pool:
                continue
            if qrel.query_id not in qid_to_relevant_documents:
                qid_to_relevant_documents[qrel.query_id] = set()
            qid_to_relevant_documents[qrel.query_id].add(qrel.doc_id)
        ret = set()

        document_added = True
        while document_added and len(ret) < self.target_size:
            document_added = False
            for qid in qid_to_relevant_documents:
                if len(qid_to_relevant_documents[qid]) <= 0:
                    continue

                doc = qid_to_relevant_documents[qid].pop()
                document_added = True
                ret.add(doc)
                if len(ret) >= self.target_size:
                    break

        random_docs = self.random_documents(ir_datasets_id)

        while len(random_docs) > 0 and len(ret) < self.target_size:
            ret.add(random_docs.pop())

        return ret

    def __str__(self) -> str:
        return f"loft-{self.target_size}"
