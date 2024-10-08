build-docker-image:
	docker build -t corpus-subsampling -f .devcontainer/Dockerfile .

docker-bash:
	docker run --rm -ti -v ${PWD}:/app -w /app --entrypoint bash corpus-subsampling
