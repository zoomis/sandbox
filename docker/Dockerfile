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

FROM openjdk:8-jre

# Install some utilities
RUN apt-get update && \
    apt-get install -y netcat \
                    python \
                    supervisor \
                    postgresql sudo \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /components

# Pulsar
RUN wget http://mirrors.ocf.berkeley.edu/apache/incubator/pulsar/pulsar-1.19.0-incubating/apache-pulsar-1.19.0-incubating-bin.tar.gz && \
    tar xvzf apache-pulsar-1.19.0-incubating-bin.tar.gz && \
    mv apache-pulsar-1.19.0-incubating /pulsar && \
    rm apache-pulsar-1.19.0-incubating-bin.tar.gz

# Heron Client
RUN wget https://github.com/twitter/heron/releases/download/0.15.0/heron-client-install-0.15.0-ubuntu.sh && \
    chmod +x heron-client-install-0.15.0-ubuntu.sh && \
    ./heron-client-install-0.15.0-ubuntu.sh && \
    rm ./heron-client-install-0.15.0-ubuntu.sh

# Heron Tools
RUN wget https://github.com/twitter/heron/releases/download/0.15.0/heron-tools-install-0.15.0-ubuntu.sh && \
    chmod +x heron-tools-install-0.15.0-ubuntu.sh && \
    ./heron-tools-install-0.15.0-ubuntu.sh && \
    rm ./heron-tools-install-0.15.0-ubuntu.sh


COPY heron-starter-0.0.1-SNAPSHOT-jar-with-dependencies.jar /components

WORKDIR /scripts

### Pulsar dashboard dependencies
RUN wget https://bootstrap.pypa.io/get-pip.py && python get-pip.py && rm get-pip.py
RUN pip install Django psycopg2 pytz requests
COPY pulsar-dashboard/postgresql.conf /etc/postgresql/9.6/main/

# Configure supervisor
COPY pulsar-dashboard/pulsar-dashboard.conf /etc/supervisor/conf.d/
COPY pulsar-dashboard/django /pulsar/django

# Setup database and create tables
RUN sudo -u postgres /etc/init.d/postgresql start && \
    sudo -u postgres psql --command "CREATE USER docker WITH PASSWORD 'docker';" && \
    sudo -u postgres createdb -O docker pulsar_dashboard && \
    cd /pulsar/django && \
    ./manage.py migrate && \
    sudo -u postgres /etc/init.d/postgresql stop

# Collect all static files needed by Django in a
# single place. Needed to run the app outside the
# Django test web server
RUN cd /pulsar/django && ./manage.py collectstatic --no-input
ENV SERVICE_URL http://localhost:8080/
### end pulsar dashboard

COPY streamlio.conf /etc/supervisor/conf.d/
COPY apply-config-from-env.py /pulsar/bin

# Settings for lower memory consumption
ENV PULSAR_MEM '" -Xmx512M -XX:MaxDirectMemorySize=512M"'
ENV PULSAR_GC '" -XX:+UseG1GC"'
ENV managedLedgerCacheSizeMB 16

ENV BOOKIE_MEM '" -Xmx512M -XX:MaxDirectMemorySize=512M"'
ENV BOOKIE_GC '" -XX:+UseG1GC"'
ENV dbStorage_writeCacheMaxSizeMb 8
ENV dbStorage_readAheadCacheMaxSizeMb 0
ENV dbStorage_readAheadCacheBatchSize 0
ENV dbStorage_rocksDB_blockCacheSize=16777216
ENV dbStorage_rocksDB_writeBufferSizeMB=4
ENV statsProviderClass org.apache.bookkeeper.stats.PrometheusMetricsProvider
ENV prometheusStatsHttpPort 8001

ENTRYPOINT ["supervisord", "-n"]
