###

In progress:
- corpus graph in ssh.webis.de
- corpus graph on betaweb012
- materializing corpus on betaweb012
- retrieval on gammaweb02
- retrieval on gammaweb07
- retrieval on gammaweb09

###




sudo docker pull docker.io/webis/ir-benchmarks-submissions:tira-ir-starter-pyterrier-0-0-1-tira-docker-software-id-plastic-cabin


sudo docker run --rm -ti --entrypoint bash -w /app/corpus_subsampling/carbon_footprints -v /mnt/ceph/storage/data-tmp/current/kibi9872/conf25-corpus-subsampling/:/app docker.io/webis/ir-benchmarks-submissions:tira-ir-starter-pyterrier-0-0-1-tira-docker-software-id-calm-bucket


sudo docker run --rm -ti --entrypoint bash -w /app/corpus_subsampling/carbon_footprints -v /mnt/ceph/storage/data-tmp/current/kibi9872/conf25-corpus-subsampling/:/app webis/reneuir-2024-submissions:reneuir-baselines-plaid-x-latest-tira-docker-software-id-breezy-crate


./run-plaid-pipelines.py msmarco-passage/trec-dl-2019/judged loft-1000 hltcoe/plaidx-large-eng-tdist-mt5xxl-engeng

./run-plaid-pipelines.py msmarco-passage/trec-dl-2019/judged loft-1000 colbert-ir/colbertv2.0

./run-plaid-pipelines.py msmarco-passage/trec-dl-2019/judged loft-1000 colbert-ir/colbertv1.9


sudo docker run --rm -ti --entrypoint bash -w /app/corpus_subsampling/carbon_footprints -v /mnt/ceph/storage/data-tmp/current/kibi9872/conf25-corpus-subsampling/:/app docker.io/webis/ir-benchmarks-submissions:tira-ir-starter-beir-0-0-1-multi-qa-minilm-l6-dot-v1-tira-docker-software-id-plastic-tangerine

./run-beir-pipelines.py msmarco-passage/trec-dl-2019/judged loft-1000 sentence-transformers/msmarco-roberta-base-ance-firstp


pip3 install codecarbon click
export CUDA_VISIBLE_DEVICES=1

