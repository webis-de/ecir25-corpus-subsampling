rsync -avh --ignore-times data/unprocessed/ kibi9872@ssh.webis.de:/mnt/ceph/storage/data-tmp/current/kibi9872/conf25-corpus-subsampling/data/unprocessed/

rsync -avh --ignore-times data/processed/ kibi9872@ssh.webis.de:/mnt/ceph/storage/data-tmp/current/kibi9872/conf25-corpus-subsampling/data/processed/

# ECIR25 (Under Review): Corpus-Subsampling for Green and Robust Retrieval Evaluation

## Development

A dev container is prepared, if you have docker installed, just open this repo in VS Code which starts the environment in a docker container with requirements already installed.

Run tests via:

```
pytest
```

Code quality via:
```
black .
isort .
flake8
```

## Carbon Footrpint Experiments

The carbon footprint experiments first index the subcorpus and subsequently retrieve for all queries for this corpus against the index, storing the resulting run file while measuring the carbon footprint of the complete process from indexing to retrieval end-to-end.

```
wget https://cloud.uni-jena.de/s/X24iwDC4tEptsN2/download/materialized-subcorpora.zip
```

|Approach      |Type     |Responsible|
|--------------|---------|-----------|
|BM25          | Lexical | Maik      |
|DirichletLM   | Lexical | Maik      |
|TF-IDF        | Lexical | Maik      |
|ANCE          | Dense   | Maik      |
|Splaid        | Sparse  | Maik      |
|ColBERT       | Late    |           |
|Splade        |         |           |
| ...          | ...     | ...       |
| ...          | ...     | ...       |

Approaches and how we can split them:


### Materialization of Corpora

```
export IR_DATASETS_HOME=/mnt/ceph/tira/state/ir_datasets/

python3 corpus_subsampling/carbon_footprints/materialize_corpora.py clueweb09/en/trec-web-2012

python3 corpus_subsampling/carbon_footprints/materialize_corpora.py clueweb12/trec-web-2014

python3 corpus_subsampling/carbon_footprints/materialize_corpora.py msmarco-passage/trec-dl-2019/judged

python3 corpus_subsampling/carbon_footprints/materialize_corpora.py disks45/nocr/trec-robust-2004

```

### TODOS

- Use deduplicated versions of the corpora and also apply the other pre-processing steps as mentioned before.

### Citation

TBD.
