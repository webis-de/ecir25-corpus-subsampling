#!/usr/bin/env bash

set -e

RETRIEVAL_SYSTEMS=("BM25" "DirichletLM" "PL2" "TF_IDF" "DPH" "Hiemstra_LM" "DLH" "DFIZ" "IFB2" "InB2")
DATASETS=("msmarco-passage/trec-dl-2019/judged")
SUBCORPORA=("loft-1000" "re-rank-top-1000-bm25" "top-100-run-pool" "top-25-run-pool" "loft-10000" "top-10-run-pool" "top-1000-run-pool" "top-50-run-pool")

for retrieval_system in "${RETRIEVAL_SYSTEMS[@]}"; do
    for dataset in "${DATASETS[@]}"; do
        for subcorpus in "${SUBCORPORA[@]}"; do
            echo "${retrieval_system} on ${dataset} with ${subcorpus}"
	    ./run-pyterrier-pipelines.py $dataset $subcorpus $retrieval_system
        done
    done
done
