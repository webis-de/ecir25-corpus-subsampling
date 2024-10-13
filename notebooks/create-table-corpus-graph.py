#!/usr/bin/env python3
import ir_datasets
from tqdm import tqdm
import gzip
import json
from statistics import mean

DATASETS = [
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

ret = {}

all_neighbours = {}

for dataset_name in tqdm(DATASETS):
    corpus = dataset_name.split('/')[0]
    if corpus not in all_neighbours:
        all_neighbours[corpus] = {}
    with open(f'data/processed/corpus-graph/{dataset_name.replace("/", "-")}.jsonl') as f:
        for l in f:
            l = json.loads(l)

            all_neighbours[corpus][l['docno']] = l['similar_documents']

def neighbours(dataset_name, documents, k):
    neighbours_for_corpus = all_neighbours[dataset_name.split('/')[0]]

    ret = set()
    for doc in documents:
        if doc not in neighbours_for_corpus:
            continue
        for sim in neighbours_for_corpus[doc][:k]:
            ret.add(sim)

    return ret

def analysis(dataset_name, documents):
    dataset = ir_datasets.load(dataset_name)
    relevant_documents = set([i.doc_id for i in dataset.qrels_iter() if i.relevance > 0])
    ret = {}
    for k in [0, 5, 10, 15]:
        docs = set()
        docs.update(documents)
        docs.update(neighbours(dataset_name, [i for i in documents if i in relevant_documents], k))

        ret[k] = {'additional-documents': len(docs) - len(documents), 'completeness': len([i for i in docs if i in relevant_documents])/len(relevant_documents)}
    return ret


for dataset_name in tqdm(DATASETS):
    corpus = dataset_name.split('/')[0]

    if corpus not in ret:
        ret[corpus] = {}

    with gzip.open(f'data/processed/sampled-corpora/{dataset_name.replace("/", "-")}.json.gz') as f:
        f = json.load(f)
        for group in f.keys():
            for sampling in f[group].keys():
                if sampling != 'top-10-run-pool' and sampling != 'top-100-run-pool' and sampling != 'top-1000-run-pool':
                    continue
                if sampling not in ret[corpus]:
                    ret[corpus][sampling] = {}

                for k, v in analysis(dataset_name, f[group][sampling]).items():
                    if k not in ret[corpus][sampling]:
                        ret[corpus][sampling][k] = []
                    
                    ret[corpus][sampling][k] += [v]

for corpus in ret:
    for sampling in ret[corpus]:
        for k in ret[corpus][sampling]:
            additional_documents = mean([i['additional-documents'] for i in ret[corpus][sampling][k]])
            completeness = mean([i['completeness'] for i in ret[corpus][sampling][k]])
            ret[corpus][sampling][k] = {'additional-documents': additional_documents, 'completeness': completeness}

def line(depth, dataset):
    l = []

    for k in [0, 5, 10, 15]:
        k = ret[dataset][f'top-{depth}-run-pool'][k]
        l += [str(int(k['additional-documents'])), "{:.3f}".format(k['completeness'])]

    return ' & '.join(l)

print("""
\\midrule

\\parbox[t]{3mm}{\\multirow{4}{*}{\\rotatebox[origin=c]{90}{Pool$_{J}$}}} & ClueWeb09 & """ + line(10, 'clueweb09') + """ \\\\

& ClueWeb12 & """ + line(10, 'clueweb12') + """ \\\\

& MS~MARCO & """ + line(10, 'msmarco-passage') + """ \\\\

& Robust04 & """ + line(10, 'disks45') + """ \\\\


\\midrule

\\parbox[t]{3mm}{\\multirow{4}{*}{\\rotatebox[origin=c]{90}{Pool$_{100}$}}} & ClueWeb09 & """ + line(100, 'clueweb09') + """ \\\\

& ClueWeb12 & """ + line(100, 'clueweb12') + """ \\\\

& MS~MARCO & """ + line(100, 'msmarco-passage') + """ \\\\

& Robust04 & """ + line(100, 'disks45') + """ \\\\


\\midrule

\\parbox[t]{3mm}{\\multirow{4}{*}{\\rotatebox[origin=c]{90}{Pool$_{1000}$}}} & ClueWeb09 & """ + line(1000, 'clueweb09') + """ \\\\

& ClueWeb12 & """ + line(1000, 'clueweb12') + """ \\\\

& MS~MARCO & """ + line(1000, 'msmarco-passage') + """ \\\\

& Robust04 & """ + line(1000, 'disks45') + """ \\\\

\\bottomrule
""")


