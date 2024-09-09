FROM ubuntu:noble

RUN apt-get update \
	&& apt-get install -y curl python3 python3-pip git

ADD setup.cfg pyproject.toml /tmp/

RUN pip3 install -e /tmp/[dev,test] --break-system-packages \
	&& rm -Rf /tmp/* \
	&& pip cache remove '*'
