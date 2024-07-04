FROM python:3.10-slim-bookworm

LABEL maintainer="Thibault Godouet <yo8192@users.noreply.github.com>"

WORKDIR /usr/src/app

# python Confuse requires the home to exist:
RUN useradd -u 9923 octo2influx --create-home

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY octo2influx.py loop.sh ./

USER octo2influx

ENV FREQ=1h
ENTRYPOINT [ "bash", "loop.sh" ]
