FROM dockerimages/ubuntu-core:14.04

RUN apt-get update && \
  apt-get upgrade -y && \
  apt-get install -y \
  	--no-install-recommends \
    python2.7 \
    build-essential \
    autotools-dev \
    libtool \
    gcc \
    g++ \
  	python2.7-dev \
  	python-pip \
  	libboost-program-options-dev \
  	libboost-python-dev \
  	git \
  	zlib1g-dev &&\
  apt-get clean -y

RUN mkdir /opt/src
WORKDIR /opt/src
RUN git clone https://github.com/JohnLangford/vowpal_wabbit.git
RUN git clone https://github.com/edenhill/librdkafka.git

WORKDIR /opt/src/vowpal_wabbit
# RUN ./configure
RUN make
RUN make install

WORKDIR /opt/src/librdkafka
RUN git checkout tags/0.9.1
RUN ./configure
RUN make
RUN make install

# Install minteressa package
WORKDIR /opt/src
COPY ../minteressa ./minteressa
WORKDIR /opt/src/minteressa
RUN python setup.py

# Instal VW python binding
RUN pip install kafka-python confluent-kafka datasketch functools32 vowpalwabbit

# Cleanup
WORKDIR /opt/src
RUN rm minteressa -Rf
RUN rm vowpal_wabbit -Rf
RUN rm librdkafka -Rf
RUN rm -rf /var/lib/apt/lists/*

RUN apt-get -y remove --purge git python2.7-dev build-essential python-pip libboost-python-dev libboost-program-options-dev
RUN apt-get -y autoremove

ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib
CMD ["python", "/opt/python/active-learner.py"]