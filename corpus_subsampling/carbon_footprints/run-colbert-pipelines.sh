#!/usr/bin/env bash

set -e

RETRIEVAL_SYSTEMS=("hltcoe/plaidx-large-eng-tdist-mt5xxl-engeng" "colbert-ir/colbertv2.0" "colbert-ir/colbertv1.9")
DATASETS=("msmarco-passage/trec-dl-2019/judged" "msmarco-passage/trec-dl-2020/judged" "disks45/nocr/trec-robust-2004" "clueweb09/en/trec-web-2012" "clueweb12/trec-web-2014")
DATASETS=("msmarco-passage/trec-dl-2019/judged" "msmarco-passage/trec-dl-2020/judged" "disks45/nocr/trec-robust-2004" "clueweb12/trec-web-2014")
SUBCORPORA=("loft-1000" "re-rank-top-1000-bm25" "top-100-run-pool" "top-25-run-pool" "loft-10000" "top-10-run-pool" "top-50-run-pool" "top-1000-run-pool")

for subcorpus in "${SUBCORPORA[@]}"; do
    for dataset in "${DATASETS[@]}"; do
        for retrieval_system in "${RETRIEVAL_SYSTEMS[@]}"; do
            echo "${retrieval_system} on ${dataset} with ${subcorpus}"
	    ./run-plaid-pipelines.py $dataset $subcorpus $retrieval_system
        done
    done
done
