#!/usr/bin/env python3

from ir_datasets_subsample import register_subsamples
import ir_datasets
from tqdm import tqdm

register_subsamples()
dataset = ir_datasets.load("corpus-subsamples/clueweb12")
for doc in tqdm(dataset.docs_iter(), total=dataset.docs_count()):
    pass
