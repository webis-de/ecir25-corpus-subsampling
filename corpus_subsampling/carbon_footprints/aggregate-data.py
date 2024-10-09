#!/usr/bin/env python3
import click
from trectools import TrecRun, TrecQrel, TrecEval
from pathlib import Path
from glob import glob
import pandas as pd
from tqdm import tqdm
import numbers
from statistics import mean
import rbo

qrels = {}
rbo_ranks = {}

def run_eval(run, truth_run, depth=10):
    if truth_run not in qrels:
        q = TrecQrel()
        q.qrels_data = []
        rbo_ranks[truth_run] = {}
        for _, i in TrecRun(truth_run).run_data.iterrows():
            if i['rank'] > depth:
                continue
            if i["query"] not in rbo_ranks[truth_run]:
                rbo_ranks[truth_run][i["query"]] = []
            q.qrels_data += [{"query": i["query"], "q0": "Q0", "docid": i["docid"], "rel": 1}]
            rbo_ranks[truth_run][i["query"]] += [i["docid"]]

        q.qrels_data = pd.DataFrame(q.qrels_data)
        qrels[truth_run] = q

    run = TrecRun(run)
    te = TrecEval(run, qrels[truth_run])
    rbo_scores = []
    for qid, group in run.run_data.groupby("query"):
        r1 = group.sort_values("score", ascending=False)['docid'].values.tolist()
        rbo_scores += [rbo.RankingSimilarity(rbo_ranks[truth_run][qid], r1).rbo()]
    return {"Recall@10": te.get_recall(depth), "RBO@10": mean(rbo_scores)}

def load_emissions(directory):
    ret = {}
    aggregated = {}
    for i in glob(f'{directory}/**/emissions.csv'):
        result = {'type': i.split('/')[-2]}
        df = pd.read_csv(i)
        assert len(df) == 1
        for k,v in df.iloc[0].to_dict().items():
            result[k] = v
            if isinstance(v, numbers.Number):
                aggregated[k] = aggregated.get(k, 0) + v

        ret[result['type']] = result

    ret['aggregated'] = aggregated
    return ret

def process_dataset(data_dir, dataset_id):
    ret = []
    run_path = f'{data_dir}/{dataset_id.replace("/", "-")}/**/**/**/run.txt'
    for run in tqdm(glob(run_path), f'Process {dataset_id}'):
        result = {'sampling': run.split('/')[-3], 'approach': run.split('/')[-4], 'dataset': dataset_id}
        truth_run = run.replace(f'/{result["sampling"]}/', '/top-1000-run-pool/')
        for k, v in run_eval(run, truth_run).items():
            result[k] = v
        for k, v in load_emissions(run.replace('run/run.txt', '')).items():
            result[k] = v
        ret += [result]
    return ret

@click.command('aggregate')
@click.option('--data-dir', type=Path, default=Path('data/processed/carbon-footprints/'))
@click.option('--output-file', type=Path, default=Path('data/processed/carbon-footprints/aggregated.jsonl'))
@click.option('--datasets', type=list, default=['disks45/nocr/trec-robust-2004', 'msmarco-passage/trec-dl-2019/judged', 'msmarco-passage/trec-dl-2020/judged', 'clueweb12/trec-web-2014'])
def main(data_dir, datasets, output_file):
    ret = []
    for d in datasets:
        ret += process_dataset(data_dir, d)

    pd.DataFrame(ret).to_json(output_file, lines=True, orient='records')

if __name__ == '__main__':
    main()

