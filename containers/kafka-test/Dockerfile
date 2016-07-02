FROM dockerimages/ubuntu-core:trusty

RUN apt-get update \
  && apt-get upgrade -y \
  && apt-get clean -y

RUN apt-get install git python2.7 python2.7-dev python-pip build-essential

WORKDIR /opt
RUN rm -Rf src
RUN mkdir src run

# Install minteressa.* modules source
WORKDIR /opt/src
RUN git clone https://github.com/malberich/pgds-etl-filters.git
RUN cd pgds-etl-filters && git pull origin master

RUN  pip install -r pgds-etl-filters/requirements.txt

RUN cd pgds-etl-filters && python setup.py build && python setup.py install

### modify those script files in order to run yours ###
RUN cp pgds-etl-filters/examples/kafka-test.py /opt/run/kafka-test.py

WORKDIR /opt
RUN rm -Rf src

### TODO check for Kafka service
### This script is expected to check for kafka availability ###
CMD ["python", "/opt/run/kafka-test.py"]
