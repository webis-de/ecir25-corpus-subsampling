# ECIR25: Corpus Subsampling: Estimating the Effectiveness of Neural Retrieval Models on Large Corpora

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

If you use the results of the paper or the subsampling approaches or our prepared subsamples, please cite this paper:

```
@InProceedings{froebe:2025c,
  address =                     {Berlin Heidelberg New York},
  author =                      {Maik Fr{\"o}be and Andrew Parry and Harrisen Scells and Shuai Wang and Shengyao Zhuang and Guido Zuccon and Martin Potthast and Matthias Hagen},
  booktitle =                   {Advances in Information Retrieval - 47th European Conference on Information Retrieval, {ECIR} 2025, Lucca, Italy, April 6-10, 2025, Proceedings, Part {I}},
  codeurl =                     {https://github.com/webis-de/ecir25-corpus-subsampling},
  editor =                      {Claudia Hauff and Craig Macdonald and Dietmar Jannach and Gabriella Kazai and Franco Maria Nardini and Fabio Pinelli and Fabrizio Silvestri and Nicola Tonellotto},
  month =                       apr,
  publisher =                   {Springer},
  series =                      {Lecture Notes in Computer Science},
  volume =                      {15572},
  pages =                       {453--471},
  doi =                         {10.1007/978-3-031-88708-6\_29},
  site =                        {Lucca, Italy},
  title =                       {{Corpus Subsampling: Estimating the Effectiveness of Neural Retrieval Models on Large Corpora}},
  year =                        2025
}
```

