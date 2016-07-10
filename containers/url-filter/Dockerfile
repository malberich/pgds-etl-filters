FROM dockerimages/ubuntu-core:trusty

RUN apt-get update \
  && apt-get upgrade -y \
  && apt-get clean -y \
  && apt-get autoremove -y


RUN apt-get install git python2.7 python2.7-dev python-pip build-essential

WORKDIR /opt
RUN rm -Rf src
RUN mkdir src run

# Install minteressa.* modules source
WORKDIR /opt/src
RUN git clone https://github.com/malberich/pgds-etl-filters.git

RUN git clone https://github.com/edenhill/librdkafka.git
WORKDIR /opt/src/librdkafka
RUN ./configure
RUN make
RUN make install


WORKDIR /opt/src/pgds-etl-filters
RUN git pull origin master
RUN pip install -r requirements.txt

RUN python setup.py build && python setup.py install

### modify those script files in order to run yours ###
RUN cp examples/url-filter.py /opt/run/url-filter.py
ADD wait-for-it.sh /opt/run/wait-for-it.sh

WORKDIR /opt
RUN rm -Rf src

RUN apt-get remove -y python-pip git python2.7-dev build-essential
RUN rm -rf /var/lib/apt/lists/*

ENV LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/local/lib"

### TODO check for Kafka service
### This script is expected to check for kafka availability ###
CMD ["sleep", "90", ";", "python", "/opt/run/url-filter.py"]
