# Corpus Subsampling Expansion for ir_datasets

We prepare a small pip-installable extension for ir_datasets that allows to use our prepared subsamples directly via ir_datasets.

Install via:

```
pip3 install .
```

## Incorporate the data

We expect that the subsamples are stored as zip in `~/.ir_datasets/corpus-subsamples/`, create this directory via:

```
mkdir -p ~/.ir_datasets/corpus-subsamples/
```

Next, download the `clueweb09-subcorpus.zip` and `clueweb12-subcorpus.zip` files and store them in `~/.ir_datasets/corpus-subsamples/`.

**Attention:** The files `clueweb09-subcorpus.zip` and `clueweb12-subcorpus.zip` are currently not yet publicly available, we are currently in discussion with the ClueWeb team on how to host those files.

## Ensure your installation is valid

Ensure that everything works as expected:

```
pytest tests/test_fast_*
```

This runs only fast tests, see below to run all tests.

## Usage

In general, you can use it as any ir_dataset (still, some methods might need to be implemented). For instance, the notebook [retrieval-example.ipynb](retrieval-example.ipynb) shows how retrieval experiments can be done. In general, the usage would be like:

```
from ir_datasets_subsample import register_subsamples
import ir_datasets

register_subsamples()
dataset = ir_datasets.load("corpus-subsamples/clueweb12")
dataset.docs_iter()
```

## Run complete test suite (slow)

you can run the full test suite (takes a while as all documents are parsed repeatedly) via:

```
pytest tests
```

