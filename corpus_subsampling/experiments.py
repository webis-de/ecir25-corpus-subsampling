import gzip
import json
from pathlib import Path

import ir_datasets
import pandas as pd
from tqdm import tqdm
from trectools import TrecEval, TrecQrel, TrecRun

from corpus_subsampling.run_files import IR_DATASET_IDS, Runs, filter_runs
from corpus_subsampling.sampling import JudgmentPoolCorpusSampler, RunPoolCorpusSampler

RUN_FILE_CACHE = {}


def load_run_file(run_file_name: str) -> TrecRun:
    global RUN_FILE_CACHE

    if run_file_name not in RUN_FILE_CACHE:
        RUN_FILE_CACHE[run_file_name] = TrecRun(run_file_name)

    return RUN_FILE_CACHE[run_file_name]


def runs_for_dataset(ir_dataset: str):
    runs = set()
    for r in Runs(ir_dataset).all_runs().values():
        runs.update(r)

    return {r: load_run_file(r) for r in tqdm(runs, f"Load runs for {ir_dataset}.")}


def copy_trec_run(run: TrecRun):
    ret = TrecRun()
    ret.run_data = run.run_data.copy()

    return ret


def get_subcorpora(ir_dataset: str, subcorpus_file: Path):
    if not subcorpus_file.exists():
        runs = runs_for_dataset(ir_dataset)

        sampling_approaches = [
            JudgmentPoolCorpusSampler(),
            RunPoolCorpusSampler(depth=50),
            RunPoolCorpusSampler(depth=100),
            RunPoolCorpusSampler(depth=1000),
        ]

        ret = {}

        for group, runs_of_group in tqdm(Runs(ir_dataset).all_runs().items(), "Process groups"):
            runs_except_group = [v for k, v in runs.items() if k not in runs_of_group]
            ret[group] = {}

            assert len(runs_of_group) > 0
            assert len(runs_except_group) == len(runs) - len(runs_of_group)

            for sampling_approach in sampling_approaches:
                ret[group][str(sampling_approach)] = [
                    i for i in sampling_approach.sample_corpus(ir_dataset, runs_except_group)
                ]

        with gzip.open(subcorpus_file, "wt") as f:
            json.dump(ret, f, indent=2)

    with gzip.open(subcorpus_file, "rt") as f:
        return json.load(f)


def qrels_on_sub_corpus(documents: set[str], ir_dataset: str) -> TrecQrel:
    skipped = 0
    ret = TrecQrel()
    ret.qrels_data = []

    for qrel in ir_datasets.load(ir_dataset).qrels_iter():
        if qrel.doc_id not in documents:
            skipped += 1
            continue

        ret.qrels_data += [{"query": qrel.query_id, "q0": "0", "docid": qrel.doc_id, "rel": qrel.relevance}]

    ret.qrels_data = pd.DataFrame(ret.qrels_data)

    return ret, skipped


def evaluation_on_sub_corpus(run: TrecRun, documents: set[str], ir_dataset: str):
    qrels, skipped = qrels_on_sub_corpus(documents, ir_dataset)
    te = TrecEval(run, qrels)

    return {"ndcg@10": te.get_ndcg(depth=10), "unjudged@10": te.get_unjudged(depth=10), "skipped_qrels": skipped}


def run_experiment(ir_dataset: str, subcorpus_file: Path):
    subcorpora = get_subcorpora(ir_dataset, subcorpus_file)
    runs = runs_for_dataset(ir_dataset)
    team_to_run_names = Runs(ir_dataset).all_runs()
    ret = {}

    ground_truth_corpus = set()
    for run in runs.values():
        ground_truth_corpus.update(run.run_data['docid'].unique())
    
    ground_truth_evaluation = {}
    for run_name, run in runs.items():
        ground_truth_evaluation[run_name] = evaluation_on_sub_corpus(run, ground_truth_corpus, ir_dataset)

    for team in tqdm(subcorpora, "Process subcorpora"):
        ret[team] = {}
        for subsampling_name, subsampling_corpus in subcorpora[team].items():
            subsampling_corpus = set(subsampling_corpus)
            leave_one_team_out_runs = {k: copy_trec_run(v) for k, v in runs.items()}
            complete_corpus = set([i for i in subsampling_corpus])

            for run_name in team_to_run_names[team]:
                leave_one_team_out_runs[run_name] = filter_runs(leave_one_team_out_runs[run_name], subsampling_corpus)
                complete_corpus.update(list(leave_one_team_out_runs[run_name].run_data["docid"].unique()))

            ret[team][subsampling_name] = {}

            # now evaluate on complete corpus vs. subsampling_corpus
            for run_name, run in leave_one_team_out_runs.items():
                ret[team][subsampling_name][run_name] = {
                    "evaluation-with-post-judgments": evaluation_on_sub_corpus(run, complete_corpus, ir_dataset),
                    "evaluation-without-post-judgments": evaluation_on_sub_corpus(run, subsampling_corpus, ir_dataset),
                    "ground-truth-evaluation": ground_truth_evaluation
                }

    with gzip.open(Path("data") / "processed" / "experiment-evaluation.json.gz", "wt") as f:
        json.dump(ret, f, indent=2)


if __name__ == "__main__":
    for dataset in IR_DATASET_IDS:
        subcorpus_file = (
            Path("data") / "processed" / "sampled-corpora" / (dataset.replace("/", "-").lower() + ".json.gz")
        )
        run_experiment(dataset, subcorpus_file)
