FROM bitnami/spark:3.1.2
USER root
COPY ../jars /opt/bitnami/spark/jars
COPY spark_dockerfile/spark-env.sh  /opt/bitnami/spark/conf

RUN apt update \
    && apt-get install -y wget build-essential  libreadline-gplv2-dev  libncursesw5-dev  libssl-dev  libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev libffi-dev zlib1g-dev && \
    cd /usr/src && \
    wget https://www.python.org/ftp/python/3.8.10/Python-3.8.10.tgz && \
    tar xzf Python-3.8.10.tgz && \
    cd Python-3.8.10 && \
    ./configure --enable-optimizations && \
    make altinstall

RUN pip3.8 install --trusted-host pypi.python.org requests