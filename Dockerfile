FROM python:3.10.11

LABEL description ="crawling and upload to elastic cloud"

#open할 port
EXPOSE 80
EXPOSE 433

RUN mkdir -p /crawling
WORKDIR /crawling
COPY src .
RUN pip install -r ./requirements.txt