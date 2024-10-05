#!/usr/bin/env python3
from tira.rest_api_client import Client
from tira.third_party_integrations import ensure_pyterrier_is_loaded
tira = Client('https://api.tira.io')
import pyterrier as pt
from pathlib import Path
import ir_datasets
from tqdm import tqdm
import click
import json

DATASET_IDS = {
    'disks45/nocr/trec-robust-2004': 'disks45-nocr-trec-robust-2004-20230209-training',
    'msmarco-passage/trec-dl-2019/judged': 'msmarco-passage-trec-dl-2019-judged-20230107-training',
    'msmarco-passage/trec-dl-2020/judged': 'msmarco-passage-trec-dl-2019-judged-20230107-training',
}
ensure_pyterrier_is_loaded()

tokeniser = pt.autoclass("org.terrier.indexing.tokenisation.Tokeniser").getTokeniser()
def pt_tokenise(text):
    return ' '.join(tokeniser.getTokens(text))

def retrieval(ds):
    index = tira.pt.index('ir-benchmarks/tira-ir-starter/Index (tira-ir-starter-pyterrier)', DATASET_IDS[ds])
    return pt.BatchRetrieve(index, wmodel='BM25') % 30

@click.command('corpus-graph')
@click.argument('ir_datasets_id')
def main(ir_datasets_id):
    output_file = Path(ir_datasets_id.replace('/', '-') + '.jsonl')
    dataset = ir_datasets.load(ir_datasets_id)
    docs_store = dataset.docs_store()

    docs = set()
    processed_docs = set()
    try:
        with open(output_file, 'r') as f:
            for l in f:
                processed_docs.add(json.loads(l)['docno'])
    except: pass
    bm25 = retrieval(ir_datasets_id)
    print(f'Reuse {len(processed_docs)}.')
    for qrel in dataset.qrels_iter():
        if qrel.relevance <= 0 or qrel.doc_id in processed_docs:
            continue
        docs.add(qrel.doc_id)

    with open(output_file, 'a+') as f:
        for doc in tqdm(docs):
            doc_text = docs_store.get(doc).default_text()
            doc_text = pt_tokenise(doc_text)
            i = {'docno': doc, 'similar_documents': [i['docno'] for _, i in bm25.search(doc_text).iterrows()]}
            f.write(json.dumps(i) + '\n')
            f.flush()


#export IR_DATASETS_HOME=/mnt/ceph/tira/state/ir_datasets/
if __name__ == '__main__':
    main()

