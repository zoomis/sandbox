---
title: Streamlio sandbox
description: Interact with a single node streamlio platform.
tags:
- heron
- pulsar
- onebox
---

# Introduction

This is the repo for the sandbox for streamlio platform components , it's a docker image that's running heron , pulsar , bookkeeper and a simple topology consuming messages which are a stream of sentences on a pulsar queue which get split into words and the aggregated in time window interval to a count map and then published to an output pulsar queue.

The docker image installs pulsar , heron and bookkeeper components and then packages and deploys the simple topology that is part of this repo.


# Requirements

### Docker

We require a docker engine running on a platform to build and deploy the container image. You can install it for your specific platform form the links below.

__OSX:__ [Download Link](https://docs.docker.com/docker-for-mac/install/)

__Windows:__ [Download Link](https://docs.docker.com/docker-for-windows/install/)

__Ubuntu:__ [Download Link](https://docs.docker.com/engine/installation/linux/docker-ce/ubuntu/)

### Maven

Maven is required to build the heron topology. Please follow instruction here if not present on system.

[Maven Installation](https://maven.apache.org/install.html)

### Pulsar Client

We require the pulsar python client for running the producer and consumer scripts.
It can be installed using the python package manager as below.

```bash
pip install pulsar-client
```

# Running the Sandbox Image

### Clone the repo
```bash
git clone https://github.com/streamlio/sandbox.git
```

### Build and copy the Heron Topology
```bash
mvn package
cp target/heron-starter-0.0.1-SNAPSHOT-jar-with-dependencies.jar ./docker/
```

### Build the Docker Image
```bash
docker build docker/ -t streamlio-sandbox:latest
docker images
```

### Run a container based on the image
```bash
docker run -id --name streamlio-sandbox \
-p 9000:9000 -p 8889:8889 -p 6650:6650 -p 8080:8080 streamlio-sandbox:latest
docker ps
```

## Understanding the docker image

# Examining Running Topology

Go to Heron Tracker UI [http://localhost:8889/](http://localhost:8889/)

# Sending Input Events

Once the docker image is up and running we can start the producer from to host to send messages to the pulsar instance in the docker container.

```bash
python producer.py
```

Sample Console output

```bash
2017-08-12 12:45:17,528 INFO : Connected to Pulsar
2017-08-12 12:45:17,528 INFO : Sending Messages ...
2017-08-12 12:45:17,583 INFO : Sending message - four score and seven years ago 
2017-08-12 12:45:17,764 INFO : Sending message - the cow jumped over the moon 
2017-08-12 12:45:17,821 INFO : Sending message - i am at two with nature 
2017-08-12 12:45:17,882 INFO : Sending message - an apple a day keeps the doctor away 
2017-08-12 12:45:17,945 INFO : Sending message - snow white and the seven dwarfs 
2017-08-12 12:45:18,004 INFO : Sending message - i am at two with nature 
2017-08-12 12:45:18,063 INFO : Sending message - an apple a day keeps the doctor away 
2017-08-12 12:45:18,127 INFO : Sending message - snow white and the seven dwarfs 
2017-08-12 12:45:18,187 INFO : Sending message - the cow jumped over the moon 
2017-08-12 12:45:18,246 INFO : Sending message - four score and seven years ago 
2017-08-12 12:45:18,303 INFO : Sending message - snow white and the seven dwarfs 
2017-08-12 12:45:18,366 INFO : Sending message - an apple a day keeps the doctor away 
2017-08-12 12:45:18,427 INFO : Sending message - the cow jumped over the moon
```

# Consuming Output Events

We can now start consuming the output stream with the consumer script


```bash
python consumer.py
```

Sample Console output

```bash
2017-08-12 12:45:33,076 INFO : Created consumer
2017-08-12 12:45:35,219 INFO : Received message '{ "word" : "away" , "count" : 9 }' id='(2,0,-1)'
2017-08-12 12:45:35,232 INFO : Received message '{ "word" : "ago" , "count" : 11 }' id='(2,1,-1)'
2017-08-12 12:45:35,232 INFO : Received message '{ "word" : "jumped" , "count" : 9 }' id='(2,2,-1)'
2017-08-12 12:45:35,232 INFO : Received message '{ "word" : "seven" , "count" : 20 }' id='(2,3,-1)'
2017-08-12 12:45:35,232 INFO : Received message '{ "word" : "cow" , "count" : 9 }' id='(2,4,-1)'
2017-08-12 12:45:35,232 INFO : Received message '{ "word" : "two" , "count" : 11 }' id='(2,5,-1)'
2017-08-12 12:45:35,232 INFO : Received message '{ "word" : "years" , "count" : 11 }' id='(2,6,-1)'
2017-08-12 12:45:35,233 INFO : Received message '{ "word" : "dwarfs" , "count" : 9 }' id='(2,7,-1)'
2017-08-12 12:45:35,233 INFO : Received message '{ "word" : "apple" , "count" : 9 }' id='(2,8,-1)'
2017-08-12 12:45:35,233 INFO : Received message '{ "word" : "score" , "count" : 11 }' id='(2,9,-1)'
2017-08-12 12:45:35,244 INFO : Received message '{ "word" : "white" , "count" : 9 }' id='(2,10,-1)'
2017-08-12 12:45:35,244 INFO : Received message '{ "word" : "four" , "count" : 11 }' id='(2,11,-1)'
```
# Examine pulsar

Go to Pulsar-Dashboard [http://localhost:8000/stats/namespace/sample/standalone/ns1/](http://localhost:8000/stats/namespace/sample/standalone/ns1/)

# Shutdown container

```bash
docker kill streamlio-sandbox
```
