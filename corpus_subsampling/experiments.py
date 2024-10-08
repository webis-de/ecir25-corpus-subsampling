import gzip
import json
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import ir_datasets
import pandas as pd
from tqdm import tqdm
from trectools import TrecEval, TrecPoolMaker, TrecQrel, TrecRun
import click

from corpus_subsampling.run_files import IR_DATASET_IDS, Runs, filter_runs
from corpus_subsampling.sampling import (
    CompleteCorpusSampler,
    LoftCorpusSampler,
    ReRankBM25CorpusSampler,
    RunPoolCorpusSampler,
)

RUN_FILE_CACHE = {}
DEPTH = 1000


def load_run_file(run_file_name: str) -> TrecRun:
    global RUN_FILE_CACHE

    if run_file_name not in RUN_FILE_CACHE:
        run = TrecRun(run_file_name).run_data
        run = run.sort_values(["query", "score", "docid"], ascending=[True, False, False]).reset_index()

        run = run.groupby("query")[["query", "docid", "score", "system"]].head(DEPTH)
        run["q0"] = "Q0"

        # Make sure that rank position starts by 1
        run["rank"] = 1
        run["rank"] = run.groupby("query")["rank"].cumsum()

        RUN_FILE_CACHE[run_file_name] = TrecRun()
        RUN_FILE_CACHE[run_file_name].run_data = run

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
            ReRankBM25CorpusSampler(depth=1000),
            LoftCorpusSampler(target_size=1000),
            LoftCorpusSampler(target_size=10000),
            # JudgmentPoolCorpusSampler(), ignore, is the same as depth=10
            RunPoolCorpusSampler(depth=10),
            RunPoolCorpusSampler(depth=25),
            RunPoolCorpusSampler(depth=50),
            RunPoolCorpusSampler(depth=100),
            RunPoolCorpusSampler(depth=1000),
            CompleteCorpusSampler(runs),
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


class Experiment:
    def __init__(self, runs, runs_to_leave_out, depth, subsampling_corpus, subsampling_name, team, ir_dataset):
        super().__init__()
        self.runs = runs
        self.runs_to_leave_out = runs_to_leave_out
        self.depth = depth
        self.subsampling_corpus = subsampling_corpus
        self.subsampling_name = subsampling_name
        self.team = team
        self.ir_dataset = ir_dataset
        self.ret = {}

    def run(self):
        self.ret = {}
        leave_one_team_out_runs = {k: copy_trec_run(v) for k, v in self.runs.items()}

        for run_name in self.runs_to_leave_out:
            leave_one_team_out_runs[run_name] = filter_runs(leave_one_team_out_runs[run_name], self.subsampling_corpus)

        evaluation_with_post_judgments = EvaluationOnSubcorpus(
            trec_pool(leave_one_team_out_runs, self.depth), self.ir_dataset
        )

        for run_name in self.runs_to_leave_out:
            leave_one_team_out_runs[run_name] = filter_runs(leave_one_team_out_runs[run_name], self.subsampling_corpus)

        evaluation_without_post_judgments = EvaluationOnSubcorpus(
            trec_pool(
                {k: v for k, v in leave_one_team_out_runs.items() if k not in self.runs_to_leave_out}, self.depth
            ),
            self.ir_dataset,
        )

        self.ret = {"runs": {}, "corpus-size": len(self.subsampling_corpus)}

        for run_name, run in leave_one_team_out_runs.items():
            self.ret["runs"][run_name] = {
                "evaluation-with-post-judgments": evaluation_with_post_judgments.evaluation_on_sub_corpus(run),
                "evaluation-without-post-judgments": evaluation_without_post_judgments.evaluation_on_sub_corpus(run),
            }
        return self.ret, self.team, self.subsampling_name, self.runs_to_leave_out


class EvaluationOnSubcorpus:
    def __init__(self, documents: set[str], ir_dataset: str):
        self.qrels, self.skipped = qrels_on_sub_corpus(documents, ir_dataset)

    def evaluation_on_sub_corpus(self, run: TrecRun):
        te = TrecEval(run, self.qrels)
        ndcg_cut_10 = 0
        ndcg_cut_10_condensed = 0
        unjudged_at_10 = 0
        try:
            ndcg_cut_10 = te.get_ndcg(depth=10)
            ndcg_cut_10_condensed = te.get_ndcg(depth=10, removeUnjudged=True)
            unjudged_at_10 = te.get_unjudged(depth=10)
        except:
            pass

        return {
            "ndcg@10": ndcg_cut_10,
            "ndcg@10-condensed": ndcg_cut_10_condensed,
            "unjudged@10": unjudged_at_10,
            "skipped_qrels": self.skipped,
        }


def trec_pool(runs, depth):
    runs = list(runs.values())
    runs = [i for i in runs if len(i.run_data) > 0]
    if len(runs) == 0:
        raise ValueError("Could not find topics")

    ret = set()
    pool = TrecPoolMaker().make_pool(runs, strategy="topX", topX=depth).pool

    for docids in pool.values():
        ret.update(docids)

    return ret


def calculate_ground_truth_evaluation(ir_dataset):
    runs = runs_for_dataset(ir_dataset)

    evaluate_on_ground_truth_corpus = set()
    for qrel in ir_datasets.load(ir_dataset).qrels_iter():
        evaluate_on_ground_truth_corpus.add(qrel.doc_id)

    ret = {}
    evaluate_on_ground_truth_corpus = EvaluationOnSubcorpus(evaluate_on_ground_truth_corpus, ir_dataset)
    for run_name, run in tqdm(runs.items(), "Do ground truth eval"):
        ret[run_name] = evaluate_on_ground_truth_corpus.evaluation_on_sub_corpus(run)

    return ret


def run_experiment(ir_dataset: str, subcorpus_file: Path, eval_file: Path, depth: int = 10):
    subcorpora = get_subcorpora(ir_dataset, subcorpus_file)
    print('SubCorpora are available')
    if eval_file.exists():
        with gzip.open(eval_file, "rt") as f:
            return json.load(f)

    runs = runs_for_dataset(ir_dataset)
    team_to_run_names = Runs(ir_dataset).all_runs()
    ret = {}

    ground_truth_evaluation = calculate_ground_truth_evaluation(ir_dataset)
    ground_truth_evaluation_top_10 = {}
    print("Prepare ground truth evaluation", flush=True)
    evaluate_on_ground_truth_corpus = EvaluationOnSubcorpus(trec_pool(runs, depth), ir_dataset)

    for run_name, run in tqdm(runs.items(), "Do ground truth eval"):
        ground_truth_evaluation_top_10[run_name] = evaluate_on_ground_truth_corpus.evaluation_on_sub_corpus(run)

    with ProcessPoolExecutor(max_workers=12) as executor:
        results = []
        for team in subcorpora:
            ret[team] = {}
            for subsampling_name, subsampling_corpus in subcorpora[team].items():
                subsampling_corpus = set(subsampling_corpus)
                e = Experiment(
                    runs, team_to_run_names[team], depth, subsampling_corpus, subsampling_name, team, ir_dataset
                )
                results += [executor.submit(e.run)]

        for i in tqdm(results, f"LOGO for {ir_dataset}"):
            result, team, subsampling_name, runs_to_remove = i.result()
            for run_name, v in result["runs"].items():
                v["is-in-leave-out-group"] = run_name in runs_to_remove
                v["ground-truth-evaluation-top-10"] = ground_truth_evaluation_top_10[run_name]
                v["ground-truth-evaluation"] = ground_truth_evaluation[run_name]
            ret[team][subsampling_name] = result

    with gzip.open(eval_file, "wt") as f:
        json.dump(ret, f, indent=2)

@click.command('experiment')
@click.argument('dataset', type=click.Choice(IR_DATASET_IDS))
def main(dataset):
    print("Process", dataset)
    subcorpus_file = (
            Path("data") / "processed" / "sampled-corpora" / (dataset.replace("/", "-").lower() + ".json.gz")
        )
    eval_file = Path("data") / "processed" / ("evaluation-" + dataset.replace("/", "-").lower() + ".json.gz")
    run_experiment(dataset, subcorpus_file, eval_file)


if __name__ == "__main__":
    main()