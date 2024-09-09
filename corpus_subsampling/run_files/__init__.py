import gzip
import json
from copy import deepcopy
from glob import glob
from pathlib import Path

from tqdm import tqdm
from trectools import TrecPool, TrecRun

default_run_groups = (
    Path(__file__).parent.parent.parent.resolve()
    / "data"
    / "unprocessed"
    / "trec-system-runs"
    / "trec-system-runs-groups.json"
)

IR_DATASET_IDS = set(
    [
        "clueweb09/en/trec-web-2009",
        "clueweb09/en/trec-web-2010",
        "clueweb09/en/trec-web-2011",
        "clueweb09/en/trec-web-2012",
        "clueweb12/trec-web-2013",
        "clueweb12/trec-web-2014",
        "msmarco-passage/trec-dl-2019/judged",
        "msmarco-passage/trec-dl-2020/judged",
        "disks45/nocr/trec-robust-2004",
    ]
)


class Runs:
    def __init__(self, ir_datasets_id, run_groups=default_run_groups):
        run_groups = json.load(open(run_groups))
        for k, v in run_groups.items():
            if v["ir-datasets-id"] == ir_datasets_id:
                self.__groups = v["groups"]
                self.__path = default_run_groups.parent.parent / Path(k)

    def all_runs(self):
        ret = {}

        for run in glob(f"{self.__path}/*"):
            group = self.assign_run_to_group_or_return_none(run)
            if group is None:
                raise ValueError("Could not assign run to group", run)
            if group not in ret:
                ret[group] = []
            ret[group] += [run]

        return ret

    def assign_runs_to_groups(self, runs):
        ret = {}
        for run in runs:
            group = self.assign_run_to_group_or_return_none(run)

            if not group:
                group = self.assign_run_to_group_or_return_none(run, case_insensitive=True)

            if not group:
                raise ValueError("I cant determine a group for " + run)

            if group not in ret:
                ret[group] = []

            ret[group] += [run]
        return ret

    def assign_run_to_group_or_return_none(self, run, case_insensitive=False):
        if not self.__groups or "items" not in dir(self.__groups) or not self.__groups.items():
            return None

        ret = set([])
        for group_name, group_definition in self.__groups.items():
            # Currently, we only support group definitions via prefixes
            assert {"prefix"} == group_definition.keys()
            if isinstance(group_definition["prefix"], str):
                prefixes = [group_definition["prefix"]]
            else:
                prefixes = group_definition["prefix"]
            for prefix in prefixes:
                if ("/" + prefix) in run:
                    ret.add(group_name)
                if case_insensitive and ("/" + prefix).lower() in run.lower():
                    ret.add(group_name)

        ret = [i for i in ret]
        return ret[0] if len(ret) == 1 else None


def load_all_runs(run_dir, verbose=True):
    run_dir = Path(str(run_dir))
    ret = {}
    print("Load runs: ")
    runs = list(run_dir.glob("*"))
    if verbose:
        runs = tqdm(runs)

    for run in runs:
        ret[str(run)] = TrecRun(run)
    return ret


def normalize_run(run, depth=100):
    """
    Break score ties, etc, as in trec_eval.
    As implemented in trectools: https://github.com/joaopalotti/trectools/blob/master/trectools/trec_eval.py#L230
    """

    trecformat = (
        run.run_data.copy().sort_values(["query", "score", "docid"], ascending=[True, False, False]).reset_index()
    )
    topX = trecformat.groupby("query")[["query", "q0", "docid", "rank", "score", "system"]].head(depth)

    # Make sure that rank position starts by 1
    topX["rank"] = 1
    topX["rank"] = topX.groupby("query")["rank"].cumsum()

    ret = TrecRun()
    ret.run_data = topX

    return ret


def make_top_x_pool(list_of_runs, depth):
    pool_documents = {}

    for run in list_of_runs:
        run = normalize_run(run, depth)

        for _, i in run.run_data.iterrows():
            topic = str(i["query"])
            doc = str(i["docid"])

            if topic not in pool_documents:
                pool_documents[topic] = set([])

            pool_documents[topic].add(doc)

    return TrecPool(pool_documents)


class IncompletePools:
    def __init__(
        self, run_dir=None, group_definition_file=None, trec_identifier=None, pool_per_run_file=None, verbose=True
    ):
        self.__runs = load_all_runs(run_dir, verbose) if run_dir else None
        self.__run_file_groups = (
            Runs(group_definition_file, trec_identifier).assign_runs_to_groups(self.__runs.keys())
            if group_definition_file and trec_identifier
            else None
        )
        self.pool_per_run_file = pool_per_run_file
        self.verbose = verbose

    def pool_per_runs(self):
        if self.pool_per_run_file:
            ret = json.load(gzip.open(self.pool_per_run_file))
            self.__run_file_groups = ret["groups"]
            return ret

        ret = {"pool_entries": {"10": {}, "20": {}}, "groups": self.__run_file_groups}

        print("Create Pool Entries for all Runs.")
        runs_to_process = self.__runs.items()
        if self.verbose:
            runs_to_process = tqdm(runs_to_process)

        for run_name, run in runs_to_process:
            for depth in ret["pool_entries"].keys():
                ret["pool_entries"][depth][run_name] = self.incomplete_pools(
                    [i for i in self.__runs.keys() if i != run_name], int(depth)
                )

        return ret

    def create_all_incomplete_pools(self):
        ret = {}
        all_runs = set()
        for _, runs_to_skip in self.__run_file_groups.items():
            all_runs = all_runs.union(runs_to_skip)

        for run in all_runs:
            for pool_name, pool in self.create_incomplete_pools_for_run(run):
                ret[pool_name] = pool

        return ret

    def create_incomplete_pools_for_run(self, run):
        if not hasattr(self, "pool_per_run"):
            self.pool_per_run = self.pool_per_runs()

        for depth in ["10", "20"]:
            yield ("complete-pool-depth-" + str(depth), self.complete_pool_at_depth(depth))
            for exclusion_group_name, runs_to_skip in self.__run_file_groups.items():
                if run not in runs_to_skip:
                    continue

                pool = None

                for run_name, run_pool in self.pool_per_run["pool_entries"][depth].items():
                    if run_name in runs_to_skip:
                        continue
                    if pool is None:
                        pool = deepcopy(run_pool)

                    for topic, topic_pool in run_pool.items():
                        if topic not in pool:
                            pool[topic] = []
                        pool[topic] = pool[topic] + topic_pool

                pool = {k: sorted(list(set(v))) for k, v in pool.items()}
                yield ("depth-" + str(depth) + "-pool-incomplete-for-" + exclusion_group_name, pool)

    def complete_pool_at_depth(self, depth):
        if not hasattr(self, "pool_per_run"):
            self.pool_per_run = self.pool_per_runs()

        pool = None
        for run_name, run_pool in self.pool_per_run["pool_entries"][depth].items():
            if pool is None:
                pool = deepcopy(run_pool)

            for topic, topic_pool in run_pool.items():
                if topic not in pool:
                    pool[topic] = []
                pool[topic] = pool[topic] + topic_pool

        return {k: sorted(list(set(v))) for k, v in pool.items()}

    def incomplete_pools(self, runs_to_skip, depth):
        skipped_runs = 0
        runs_for_pooling = []

        for run_name, run in self.__runs.items():
            if run_name in runs_to_skip:
                skipped_runs += 1
                continue
            runs_for_pooling += [run]

        assert skipped_runs == len(runs_to_skip)
        return {k: sorted(list(v)) for k, v in make_top_x_pool(runs_for_pooling, depth).pool.items()}


if __name__ == "__main__":
    print(len(load_all_runs("src/main/resources/unprocessed/trec-system-runs/trec-covid/")))
