
FROM continuumio/miniconda3:23.3.1-0

ENV CONDA_DIR=/opt/conda
ENV WORKSPACE=/home/workspace
ENV PYTHONPATH ${PYTHONPATH}:${WORKSPACE}

# dev container purpose only, can stay root
USER root

RUN mkdir -p ${WORKSPACE}

# Init Dockerfile with basics requirements for pip package development
RUN apt-get update 
RUN apt-get install -y git
RUN apt-get install -y gcc
RUN pip install pytest
RUN pip install twine

# Install package requirements
COPY requirements.txt /
RUN pip install -r /requirements.txt

WORKDIR $WORKSPACE

ENTRYPOINT ["tail"]
CMD ["-f","/dev/null"]