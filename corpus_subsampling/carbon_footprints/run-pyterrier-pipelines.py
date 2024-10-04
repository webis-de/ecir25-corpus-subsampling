#!/usr/bin/env python3
import click
from pathlib import Path
from tira.third_party_integrations import ensure_pyterrier_is_loaded, normalize_run
from codecarbon import OfflineEmissionsTracker
import os
import gzip
import json
from tqdm import tqdm
import pyterrier as pt

def retrieve(queries, index, wmodel, output_directory):
    print(f'loading topics from:\t{queries}')
    queries = pt.io.read_topics(queries, 'trecxml')

    
    with OfflineEmissionsTracker(country_iso_code="DEU", output_dir=output_directory) as tracker:
        pipeline = pt.BatchRetrieve(index, wmodel=wmodel)

        result = pipeline(queries)

        print(f'writing run file to:\t{output_directory}/run.txt')
        Path(output_directory).mkdir(parents=True, exist_ok=True)
        pt.io.write_results(normalize_run(result, 1000), f'{output_directory}/run.txt', run_name=f'pyterrier.{wmodel}')

def index(documents_file, index_directory):
    documents = []
    with gzip.open(str(documents_file), 'rt') as f:
        for l in f:
            documents += [json.loads(l)]

    with OfflineEmissionsTracker(country_iso_code="DEU", output_dir=index_directory) as tracker:
        documents = tqdm(documents, 'Load Documents')

        print(f'create new index at:\t{index_directory}')
        return pt.IterDictIndexer(index_directory, meta= {'docno' : 50}).index(documents)

RESOURCES = Path(__file__).parent / '..' / '..' / 'data' / 'processed' 


@click.command('carbon-footprint')
@click.argument('ir-datasets-id')
@click.argument('sample-strategy')
@click.argument('retrieval')
def main(ir_datasets_id, sample_strategy, retrieval):
    ir_datasets_id = ir_datasets_id.replace('/', '-')
    corpus_dir = RESOURCES / 'materialized-corpora' /ir_datasets_id

    docs = corpus_dir / sample_strategy / 'documents.jsonl.gz'
    queries = corpus_dir / 'queries.xml'
    results_dir = RESOURCES / 'carbon-footprints' / ir_datasets_id / retrieval / sample_strategy
    index_dir = results_dir / 'index'
    run_dir = results_dir / 'run'
    index_dir.mkdir(parents=True, exist_ok=True)
    run_dir.mkdir(parents=True, exist_ok=True)

    if not docs.exists():
        raise ValueError(f'Does not exist: {docs}')

    if not queries.exists():
        raise ValueError(f'Does not exist: {queries}')

    ensure_pyterrier_is_loaded()
    idx = index(docs, os.path.abspath(index_dir))
    retrieve(queries, idx, retrieval, run_dir)

if __name__ == '__main__':
    main()
