# Corpus Subsampling Expansion for ir_datasets

We prepare a small pip-installable extension for ir_datasets that allows to use our prepared subsamples directly via ir_datasets.

Install via:

```
pip3 install -e .
```

Then, you can use it via:

```
from ir_datasets_subsample import register_subsamples
import ir_datasets

register_subsamples()
dataset = ir_datasets.load("corpus-subsamples/clueweb12")
dataset.docs_iter()
```

