from abc import ABC, abstractmethod

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
        return 'judgment-pool'


class RunPoolCorpusSampler(JudgmentPoolCorpusSampler):
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
        ret = super().sample_corpus(ir_datasets_id, runs)
        pool = TrecPoolMaker().make_pool(runs, strategy="topX", topX=self.depth).pool

        for docids in pool.values():
            ret.update(docids)

        return ret

    def __str__(self) -> str:
        return f'top-{self.depth}-run-pool'


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
