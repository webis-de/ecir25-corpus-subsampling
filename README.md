# ECIR25: Corpus-Subsampling for Green and Robust Retrieval Evaluation

This repository contains the code for corpus subsampling of to conduct retrieval experiments on huge web corpora like the ClueWebs at a reduced cost.

## Development

We use a dev container that ships all the dependencies.
If you have docker installed, just open this repo in VS Code which starts the environment in a docker container with requirements already installed.

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


### Materialization of Corpora

```
export IR_DATASETS_HOME=/mnt/ceph/tira/state/ir_datasets/

python3 corpus_subsampling/carbon_footprints/materialize_corpora.py clueweb09/en/trec-web-2012

python3 corpus_subsampling/carbon_footprints/materialize_corpora.py clueweb12/trec-web-2014

python3 corpus_subsampling/carbon_footprints/materialize_corpora.py msmarco-passage/trec-dl-2019/judged

python3 corpus_subsampling/carbon_footprints/materialize_corpora.py disks45/nocr/trec-robust-2004

```

### Citation

TBD.
