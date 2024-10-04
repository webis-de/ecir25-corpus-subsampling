#!/usr/bin/env python3
import gzip
import json
from pathlib import Path

import click
import ir_datasets
from tqdm import tqdm
from func_timeout import func_timeout

PROCESSED_RESOURCES = Path(__file__).parent.parent.parent / "data" / "processed"


def load(resource):
    with gzip.open(PROCESSED_RESOURCES / Path(resource), "rt") as f:
        return json.load(f)


def load_subcorpora(dataset_id):
    leave_one_group_out_corpora = load("sampled-corpora/" + dataset_id.replace("/", "-") + ".json.gz")
    ret = {}

    for group in tqdm(leave_one_group_out_corpora, "load groups"):
        for approach in leave_one_group_out_corpora[group]:
            if approach == "complete-corpus":
                continue
            if approach not in ret:
                ret[approach] = set()
            ret[approach].update(leave_one_group_out_corpora[group][approach])
    ret = [(k, v) for k, v in ret.items()]
    return sorted(ret, key=lambda i: len(i[1]))


def doc_text(docs_store, doc_id):
    return docs_store.get(doc_id).default_text()

def persist_docs(target_resource, documents):
    print('persist docs')
    with gzip.open(target_resource / 'documents.jsonl.gz', "wt") as f:
        for doc in documents:
            f.write(json.dumps(doc) + "\n")


@click.command("materialize-corpus")
@click.argument("dataset_id", type=str)
def materialize_corpus(dataset_id):
    subcorpora = load_subcorpora(dataset_id)
    dataset = ir_datasets.load(dataset_id)
    loaded_doc_texts = {}

    for group_name, docs in subcorpora:
        target_resource = "materialized-corpora/" + dataset_id.replace("/", "-") + f"/{group_name}/"

        target_resource = (PROCESSED_RESOURCES / Path(target_resource))
        target_resource.mkdir(exist_ok=True, parents=True)
        if (target_resource / 'documents.jsonl.gz').exists():
            with gzip.open(target_resource / 'documents.jsonl.gz', 'rt') as f:
                for l in tqdm(f, f'Load previous docs {group_name}'):
                    l = json.loads(l)
                    loaded_doc_texts[l['docno']] = l['text']

        documents = []
        docs_store = dataset.docs_store()

        inferences = 0
        for doc in tqdm(docs, group_name):
            if doc in loaded_doc_texts:
                documents += [{"docno": doc, "text": loaded_doc_texts[doc], "original_document": {}}]
                continue
            try:
                inferences += 1
                dt = func_timeout(60, doc_text, (docs_store, doc))
                documents += [{"docno": doc, "text": dt, "original_document": {}}]
            except:
                pass

            if inferences > 2500:
                inferences = 0
                persist_docs(target_resource, documents)

        persist_docs(target_resource, documents)


if __name__ == "__main__":
    materialize_corpus()
