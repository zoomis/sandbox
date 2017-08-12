#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

FROM openjdk:8-jdk

# Install some utilities
RUN apt-get update
RUN apt-get install -y netcat 
RUN apt-get install -y dnsutils
RUN apt-get install -y wget 
RUN apt-get install -y build-essential 
RUN apt-get install -y python python-dev python-pip python-virtualenv
RUN apt-get install -y git
RUN apt-get install -y maven

WORKDIR /components

# Pulsar
RUN wget http://mirrors.ocf.berkeley.edu/apache/incubator/pulsar/pulsar-1.19.0-incubating/apache-pulsar-1.19.0-incubating-bin.tar.gz
RUN tar xvzf apache-pulsar-1.19.0-incubating-bin.tar.gz
RUN mv apache-pulsar-1.19.0-incubating /pulsar

# Heron Client
RUN wget https://github.com/twitter/heron/releases/download/0.15.0/heron-client-install-0.15.0-ubuntu.sh
RUN chmod +x heron-client-install-0.15.0-ubuntu.sh
RUN ./heron-client-install-0.15.0-ubuntu.sh

# Heron Tools
RUN wget https://github.com/twitter/heron/releases/download/0.15.0/heron-tools-install-0.15.0-ubuntu.sh
RUN chmod +x heron-tools-install-0.15.0-ubuntu.sh
RUN ./heron-tools-install-0.15.0-ubuntu.sh

RUN git clone 


RUN touch sandbox-starter.sh
RUN echo "heron-tracker --rootpath ~/.herondata/repository/state/onenode > /var/log/heron-tracker.log 2>&1 &" >> sandbox-starter.sh
RUN echo "heron-ui > /var/log/heron-ui.log 2>&1 &" >> sandbox-starter.sh
RUN echo "heron-apiserver --base-template=local --cluster=onenode -Dheron.directory.home=/usr/local/heron > /var/log/heron-apiserver.log 2>&1" >> sandbox-starter.sh
RUN echo "sleep infinity" >> sandbox-starter.sh
RUN echo "/components/pulsar/bin/pulsar standalone" >> sandbox-starter.sh

# WORKDIR /pulsar

# VOLUME  ["/pulsar/conf", "/pulsar/data"]

# ENV PULSAR_ROOT_LOGGER=INFO,CONSOLE
