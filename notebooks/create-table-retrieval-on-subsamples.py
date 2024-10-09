#!/usr/bin/env python3
import os
import gzip
import json
import pandas as pd
from statistics import mean
from tqdm import tqdm


RETRIEVAL_PARADIGMS = {
    'multi-qa-distilbert-cos-v1': 'Bi-Encoder',
    'DPH': 'Lexical',
    'multi-qa-minilm-l6-cos-v1': 'Bi-Encoder',
    'IFB2': 'Lexical',
    'InB2': 'Lexical',
    'msmarco-minilm-l12-cos-v5': 'Bi-Encoder',
    'PL2': 'Lexical',
    'BM25': 'Lexical',
    'hltcoe-plaidx-large-eng-tdist-mt5xxl-engeng': 'Late Interaction',
    'DirichletLM': 'Lexical',
    'colbert-ir-colbertv2.0': 'Late Interaction',
    'multi-qa-mpnet-base-cos-v1': 'Bi-Encoder',
    'sentence-transformers-msmarco-roberta-base-ance-firstp': 'Bi-Encoder',
    'TF_IDF': 'Lexical',
    'DFIZ': 'Lexical',
    'msmarco-minilm-l6-cos-v5': 'Bi-Encoder',
    'DLH': 'Lexical',
    'msmarco-distilbert-base-tas-b': 'Bi-Encoder',
    'colbert-ir-colbertv1.9': 'Late Interaction',
    'Hiemstra_LM': 'Lexical',
    'msmarco-distilbert-base-v3': 'Bi-Encoder',
}

def dataset_id_to_corpus_sizes():
    if not os.path.isfile('../data/processed/carbon-footprints/dataset-sizes.json'):
        
        ret = {}
        for dataset in tqdm(["msmarco-passage/trec-dl-2019/judged", "msmarco-passage/trec-dl-2020/judged", "disks45/nocr/trec-robust-2004", "clueweb09/en/trec-web-2012", "clueweb12/trec-web-2014"]):
            ret[dataset] = {}
            with gzip.open(f'../data/processed/sampled-corpora/{dataset.replace("/", "-")}.json.gz') as f:
                f = json.load(f)
                for group in f.keys():
                    for sampling in f[group].keys():
                        if sampling not in ret[dataset]:
                            ret[dataset][sampling] = set()

                        ret[dataset][sampling].update(f[group][sampling])
                
                ret[dataset] = {k: len(v) for k, v in ret[dataset].items()}
        
        json.dump(ret, open('../data/processed/carbon-footprints/dataset-sizes.json', 'w'))

    return json.load(open('../data/processed/carbon-footprints/dataset-sizes.json'))

def to_rows(df):
    ret = []
    for sampling in df.keys():
        row = {'Sampling': sampling}
        for paradigm in df[sampling].keys():
            for dataset in df[sampling][paradigm].keys():
                row[f'{paradigm} ({dataset})'] = mean(df[sampling][paradigm][dataset])
        ret += [row]
    return pd.DataFrame(ret)

def parse_data(file_name, target_maeasure):
    raw_data = pd.read_json(file_name, lines=True)
    df = {}
    for _, i in raw_data.iterrows():
        if i['sampling'] not in df:
            df[i['sampling']] = {}
        paradigm = RETRIEVAL_PARADIGMS[i['approach']]
        if paradigm not in df[i['sampling']]:
            df[i['sampling']][paradigm] = {}
        dataset = i['dataset'].split('/')[0]

        if dataset not in df[i['sampling']][paradigm]:
            df[i['sampling']][paradigm][dataset] = []
        df[i['sampling']][paradigm][dataset] += [target_maeasure(i)]

    return df

df_eval = parse_data('data/processed/carbon-footprints/aggregated.jsonl', lambda i: i['RBO@10'])
df_eval = to_rows(df_eval)
df_eval = df_eval[df_eval['Sampling'] != 'top-1000-run-pool']

def format(entry, col):
    max_score = df_eval[col].max() -0.0001
    style = '\\bfseries' if max_score <= entry[col] else ''
    return style + (" {:.3}".format(entry[col])).replace('0.', '.')

def table_line(method):
    for _, i in df_eval.iterrows():
        if i['Sampling'] == method:
            f = lambda column: format(i.to_dict(), column)
            return '--- & --- & --- & ' + f('Bi-Encoder (clueweb12)') + ' & ' + f('Late Interaction (clueweb12)') + ' & ' + f('Lexical (clueweb12)') + ' & ' + \
                f('Bi-Encoder (msmarco-passage)') + ' & ' + f('Late Interaction (msmarco-passage)') + ' & ' + f('Lexical (msmarco-passage)') + ' & ' + \
                f('Bi-Encoder (disks45)') + ' & ' + f('Late Interaction (disks45)') + ' & ' + f('Lexical (disks45)')
    return ''

print("""
\\begin{tabular}{@{}lccc@{\\hspace{1.3em}}ccc@{\\hspace{1.3em}}ccc@{\\hspace{1.3em}}ccc@{}}
\\toprule
\\bfseries Sampling     &   \\multicolumn{3}{c@{\\hspace{1em}}}{\\bfseries ClueWeb09}  & \\multicolumn{3}{c@{\\hspace{1em}}}{\\bfseries ClueWeb12}     & \\multicolumn{3}{c@{\\hspace{1em}}}{\\bfseries MS~MARCO} & \\multicolumn{3}{c@{\\hspace{1em}}}{\\bfseries Robust04}                                              \\\\
\\cmidrule(r@{1em}){2-4}
\\cmidrule(r@{1em}){5-7}
\\cmidrule(r@{1em}){8-10}
\\cmidrule(){11-13}

& Bi-E. & Late & Lex. & Bi-E. & Late & Lex. & Bi-E. & Late & Lex. & Bi-E. & Late & Lex. \\\\

\\midrule

BM25 & """ + table_line('re-rank-top-1000-bm25') + """ \\\\


\\midrule

LOFT$_{1k}$ & """ + table_line('loft-1000') + """ \\\\
LOFT$_{10k}$ & """ + table_line('loft-10000') + """ \\\\


\\midrule

Pool$_J$ & """ + table_line('top-10-run-pool') + """ \\\\
Pool$_{25}$ & """ + table_line('top-25-run-pool') + """ \\\\
Pool$_{50}$ & """ + table_line('top-50-run-pool') + """ \\\\
Pool$_{100}$ & """ + table_line('top-100-run-pool') + """ \\\\

\\bottomrule

\\end{tabular}
""")