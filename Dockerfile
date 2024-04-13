FROM python:3.10.8
WORKDIR /home/prep-flow

RUN apt-get update && apt-get -y upgrade
RUN apt-get install -y build-essential

ENV PYTHONPATH /home/prep-flow

ADD ./requirements.txt /tmp/requirements.txt

RUN python -m pip install --upgrade pip
RUN python -m pip install -r /tmp/requirements.txt
