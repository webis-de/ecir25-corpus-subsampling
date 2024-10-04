#!/usr/bin/env python3
import click
from pathlib import Path
from codecarbon import OfflineEmissionsTracker
import os
import gzip
import json
import numpy as np
from colbert.infra import ColBERTConfig
from colbert.data import Collection
from colbert import Indexer
from tqdm import tqdm

def retrieve(queries, index, wmodel, output_directory):
    print(f'loading topics from:\t{queries}')
    queries = pt.io.read_topics(queries, 'trecxml')

    
    with OfflineEmissionsTracker(country_iso_code="DEU", output_dir=output_directory) as tracker:
        from colbert.indexing.codecs.residual import ResidualCodec
	ResidualCodec.try_load_torch_extensions(True)


        result = pipeline(queries)

        print(f'writing run file to:\t{output_directory}/run.txt')
        Path(output_directory).mkdir(parents=True, exist_ok=True)
        pt.io.write_results(normalize_run(result, 1000), f'{output_directory}/run.txt', run_name=f'pyterrier.{wmodel}')

RESOURCES = Path(__file__).parent / '..' / '..' / 'data' / 'processed' 


def index(documents_file, index_directory, model):
    documents = []
    with gzip.open(str(documents_file), 'rt') as f:
        for l in f:
            documents += [json.loads(l)]

    with OfflineEmissionsTracker(country_iso_code="DEU", output_dir=index_directory) as tracker:
        from colbert.indexing.codecs.residual import ResidualCodec
        ResidualCodec.try_load_torch_extensions(True)
        
        collection = Collection.cast([ i['text'] for i in documents])
        indexer = Indexer(checkpoint=model, config=ColBERTConfig(bsize=64, nbits=1, root='/tmp/'))

        print(f'Start Indexing {len(collection)} documents to "{output_directory}".')

        indexer.prepare(name=output_directory, collection=collection, overwrite=True)
        indexer.encode(name=output_directory, collection=collection)
        indexer.finalize(name=output_directory, collection=collection)

        doc_ids = [ l['docno'] for l in documents ]
        json.dump(doc_ids, gzip.open(f'{output_directory}/document_ids.json.gz', 'wt'))


@click.command('carbon-footprint')
@click.argument('ir-datasets-id')
@click.argument('sample-strategy')
@click.argument('retrieval')
def main(ir_datasets_id, sample_strategy, retrieval):
    ir_datasets_id = ir_datasets_id.replace('/', '-')
    corpus_dir = RESOURCES / 'materialized-corpora' /ir_datasets_id

    docs = corpus_dir / sample_strategy / 'documents.jsonl.gz'
    queries = corpus_dir / 'queries.xml'
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

    idx = index(docs, os.path.abspath(index_dir))
    #retrieve(queries, idx, retrieval, run_dir)

if __name__ == '__main__':
    main()
