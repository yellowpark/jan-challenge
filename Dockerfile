FROM ubuntu
RUN apt-get update && apt-get install -y iputils-ping

FROM python:3.7
WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
COPY send.py ./workspace

RUN chmod 0777 -R /usr/src/app

CMD "echo sleeping ; sleep 30000000"
