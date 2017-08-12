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

RUN git clone https://github.com/streamlio/sandbox.git
WORKDIR /components/sandbox
RUN mvn clean:clean
RUN mvn package

WORKDIR /scripts

RUN apt-get install -y supervisor
COPY streamlio.conf /etc/supervisor/conf.d/


### Pulsar dashboard dependencies
RUN apt-get -y install postgresql python sudo nginx
RUN pip install uwsgi Django psycopg2 pytz requests
COPY pulsar-dashboard/postgresql.conf /etc/postgresql/9.6/main/

# Configure nginx and supervisor
RUN echo "daemon off;" >> /etc/nginx/nginx.conf
COPY pulsar-dashboard/nginx-app.conf /etc/nginx/sites-available/default
COPY pulsar-dashboard/pulsar-dashboard.conf /etc/supervisor/conf.d/
COPY pulsar-dashboard/uwsgi_params /pulsar/conf/
COPY pulsar-dashboard/uwsgi.ini /pulsar/conf/
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

ENTRYPOINT ["supervisord", "-n"]
