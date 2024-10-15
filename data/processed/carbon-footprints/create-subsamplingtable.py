#!/usr/bin/env python3
import json
from statistics import mean

SIZES = {}
DOCS = {}

with open('dataset-sizes.json', 'r') as f:
    f = json.load(f)
    for k, v in f.items():
        corpus = k.split('/')[0]
        if corpus not in SIZES:
            SIZES[corpus] = {}
            DOCS[corpus] = {}

        for t in v:

            if t not in SIZES[corpus]:
                SIZES[corpus][t] = []
                DOCS[corpus][t] = []

            SIZES[corpus][t] += [v[t]['size']]
            DOCS[corpus][t] += [v[t]['documents']]

def line(approach):
    ret = []
    for corpus in ['clueweb09', 'clueweb12', 'msmarco-passage', 'disks45']:
        ret += [str(mean(DOCS[corpus][approach])), str(mean(SIZES[corpus][approach])), '---']
    return ' & ' + (' & '.join(ret))

print("""
\\midrule

BM25 """ + line('re-rank-top-1000-bm25') + """\\\\


\\midrule

LOFT$_{1k}$ """ + line('loft-1000') + """\\\\
LOFT$_{10k}$ """ + line('loft-10000') + """\\\\


\\midrule

Pool$_J$ """ + line('top-10-run-pool') + """\\\\
Pool$_{25}$ """ + line('top-25-run-pool') + """\\\\
Pool$_{50}$ """ + line('top-50-run-pool') + """\\\\
Pool$_{100}$ """ + line('top-100-run-pool') + """\\\\
Pool$_{1000}$ """ + line('top-1000-run-pool') + """ \\\\

\\bottomrule
""")

