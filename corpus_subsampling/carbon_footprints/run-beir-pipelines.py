#!/usr/bin/env python3
import click
from pathlib import Path
from codecarbon import OfflineEmissionsTracker
import os
import gzip
import json
import numpy as np
import pandas as pd
from full_ranking import main
import pandas as pd
from beir.retrieval import models
from tqdm import tqdm
from tira.third_party_integrations import persist_and_normalize_run
from beir.retrieval.search.dense import DenseRetrievalExactSearch as DRES

RESOURCES = Path(__file__).parent / '..' / '..' / 'data' / 'processed' 


def rank_all(documents_file, queries_file, output_directory, model):
    df_docs = pd.read_json(documents_file, lines=True)
    df_queries = pd.read_json(queries_file, lines=True)

    with OfflineEmissionsTracker(country_iso_code="DEU", output_dir=index_directory) as tracker:
        sbert_model = models.SentenceBERT(model)
        model = DRES(sbert_model, batch_size=128, corpus_chunk_size=50000)
        corpus = {i['docno']:{'text': i['text']} for _, i in df_docs.iterrows()}
        queries = {i['qid']: i['query'] for _, i in df_queries.iterrows()}
        
        scores = model.search(corpus=corpus, queries=queries, top_k=1000, score_function=score_function, return_sorted=True)
        ret = []

        for qid in scores:
            for doc_id in scores[qid]:
                ret += [{'qid': qid, 'Q0': 0, 'docno': doc_id, 'score': scores[qid][doc_id]}]
        
        persist_and_normalize_run(pd.DataFrame(ret), 'beir', output_directory)


@click.command('carbon-footprint')
@click.argument('ir-datasets-id')
@click.argument('sample-strategy')
@click.argument('retrieval')
def main(ir_datasets_id, sample_strategy, retrieval):
    ir_datasets_id = ir_datasets_id.replace('/', '-')
    corpus_dir = RESOURCES / 'materialized-corpora' /ir_datasets_id

    docs = corpus_dir / sample_strategy / 'documents.jsonl.gz'
    queries = corpus_dir / 'queries.jsonl'
    results_dir = RESOURCES / 'carbon-footprints' / ir_datasets_id / retrieval.lower().replace('/', '-') / sample_strategy
    run_dir = results_dir / 'run'
    run_dir.mkdir(parents=True, exist_ok=True)

    if not docs.exists():
        raise ValueError(f'Does not exist: {docs}')

    if not queries.exists():
        raise ValueError(f'Does not exist: {queries}')
        
    if (run_dir / 'run.txt').exists():
        return

    print('Start retrieval')
    idx = rank_all(docs, queries, os.path.abspath(run_dir), retrieval)

if __name__ == '__main__':
    main()
