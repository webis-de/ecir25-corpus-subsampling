#!/usr/bin/env python3
import click
from pathlib import Path
from codecarbon import OfflineEmissionsTracker
import os
import gzip
import json
import numpy as np
import pandas as pd
from colbert.infra import ColBERTConfig
from colbert.data import Collection
from colbert import Searcher
from colbert import Indexer
from tqdm import tqdm
from tira.third_party_integrations import persist_and_normalize_run

def retrieve(queries_file, index, wmodel, output_directory):
    print(f'loading topics from:\t{queries_file}')
    queries = {}
    with open(queries_file, 'r') as f:
        for l in f:
            l = json.loads(l)
            queries[l['qid']] = l['query']
    
    with OfflineEmissionsTracker(country_iso_code="DEU", output_dir=output_directory, tracking_mode='process') as tracker:
        from colbert.indexing.codecs.residual import ResidualCodec
        ResidualCodec.try_load_torch_extensions(True)

        document_ids = json.load(gzip.open(f'{index}/document_ids.json.gz'))
        searcher = Searcher(index=index)

        raw_scores = searcher.search_all(queries, k=1000)

        print(f'writing run file to:\t{output_directory}/run.txt')
        Path(output_directory).mkdir(parents=True, exist_ok=True)

        run = []

        for qid, ranking in raw_scores.items():
            for didx, _, score in ranking:
                run += [{'qid': qid, 'docno': str(document_ids[didx]), 'score': score}]

        run = pd.DataFrame(run)

        persist_and_normalize_run(run, 'plaid-x', str(output_directory))


RESOURCES = Path(__file__).parent / '..' / '..' / 'data' / 'processed' 


def index(documents_file, index_directory, model):
    documents = []
    with gzip.open(str(documents_file), 'rt') as f:
        for l in f:
            documents += [json.loads(l)]

    with OfflineEmissionsTracker(country_iso_code="DEU", output_dir=index_directory, tracking_mode='process') as tracker:
        collection = Collection.cast([ i['text'] for i in documents])
        indexer = Indexer(checkpoint=model, config=ColBERTConfig(bsize=64, nbits=1, root='/tmp/'))

        print(f'Start Indexing {len(collection)} documents to "{index_directory}".')

        indexer.prepare(name=index_directory, collection=collection, overwrite=True)
        indexer.encode(name=index_directory, collection=collection)
        indexer.finalize(name=index_directory, collection=collection)

        doc_ids = [ l['docno'] for l in documents ]
        json.dump(doc_ids, gzip.open(f'{index_directory}/document_ids.json.gz', 'wt'))
        return index_directory


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
    index_dir = results_dir / 'index'
    run_dir = results_dir / 'run'
    index_dir.mkdir(parents=True, exist_ok=True)
    run_dir.mkdir(parents=True, exist_ok=True)

    if not docs.exists():
        raise ValueError(f'Does not exist: {docs}')

    if not queries.exists():
        raise ValueError(f'Does not exist: {queries}')
        
    if (run_dir / 'run.txt').exists():
        return

    print('load torch extension')
    from colbert.indexing.codecs.residual import ResidualCodec
    ResidualCodec.try_load_torch_extensions(True)

    print('Start indexing')
    idx = index(docs, os.path.abspath(index_dir), retrieval)
    retrieve(queries, idx, retrieval, run_dir)

if __name__ == '__main__':
    main()
