
build-docker-image:
	docker build -t corpus-subsampling -f .devcontainer/Dockerfile .
