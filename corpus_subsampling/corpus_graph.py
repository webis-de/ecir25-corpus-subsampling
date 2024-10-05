#!/usr/bin/env python3
import click
from elasticsearch_dsl import connections, Search
import ir_datasets
from pathlib import Path
from nltk.corpus import stopwords
from tqdm import tqdm
import json
import math
stopwords = set(stopwords.words('english'))
num_documents = 1135207500


#pip3 install 'elasticsearch-dsl>=6.0.0,<7.0.0' --break-system-packages
connections.configure(default={
    'hosts': ['http://betaweb016.medien.uni-weimar.de:9200'],
    'retry_on_timeout': True,
    'timeout': 30
})


def tf_idf(t):
    doc_freq = 10
    if 'doc_freq' in t:
        doc_freq = t['doc_freq']
    return t['term_freq'] * math.log(num_documents / doc_freq)

def es_id(trec_id, index):
    results = Search().index(index).filter("term", warc_trec_id=trec_id).execute()

    if not results.hits or len(results.hits) != 1:
        return None

    return results.hits[0].meta["id"]


def similar_documents(trec_id, index, length=30, docs=27):
    q = doc_query(trec_id, index, length)
    if q is None:
        return []
    
    q = " ".join(q)
    
    ret = []
    for i in Search().index(index).query("match", **{"body_lang.en": q})[:docs]:
        if i["warc_trec_id"] == trec_id:
            continue
        ret += [i["warc_trec_id"]]
    return ret


def term_vector(trec_id, index):
    trec_id = es_id(trec_id, index)
    if trec_id is None:
        return None
    ret = connections.get_connection().termvectors(
        index,
        id=trec_id,
        term_statistics=True,
        field_statistics=False,
        fields=["body_lang.en"],
        doc_type='warcrecord'
    )
    assert ret["_id"] == trec_id
    if "body_lang.en" not in ret["term_vectors"]:
        return None

    ret = ret["term_vectors"]["body_lang.en"]["terms"]
    ret = {i: tf_idf(ret[i]) for i in ret}
    return {i: ret[i] for i in sorted(ret, key=lambda i: ret[i], reverse=True)}


def doc_query(trec_id, index, length=30):
    term_vec = term_vector(trec_id, index)
    if term_vec is None:
        return None
    ret = []
    for t in term_vec:
        if len(t) < 3 or t in stopwords:
            continue
        ret += [t]
    return ret[:length]

@click.command('corpus-graph')
@click.argument('ir_datasets_id')
def main(ir_datasets_id):
    output_file = Path('../data/processed/corpus-graph/' + ir_datasets_id.replace('/', '-') + '.jsonl')
    dataset = ir_datasets.load(ir_datasets_id)
    index = {'clueweb09': 'webis_warc_clueweb09_003', 'clueweb12': 'webis_warc_clueweb12_011'}[ir_datasets_id.split('/')[0]]

    docs = set()
    processed_docs = set()
    try:
        with open(output_file, 'r') as f:
            for l in f:
                processed_docs.add(json.loads(l)['docno'])
    except: pass
    print(f'Reuse {len(processed_docs)}.')
    for qrel in dataset.qrels_iter():
        if qrel.relevance <= 0 or qrel.doc_id in processed_docs:
            continue
        docs.add(qrel.doc_id)
    
    with open(output_file, 'a+') as f:
        for doc in tqdm(docs):
            i = {'docno': doc, 'similar_documents': similar_documents(doc, index)}
            f.write(json.dumps(i) + '\n')
            f.flush()


if __name__ == '__main__':
    main()

