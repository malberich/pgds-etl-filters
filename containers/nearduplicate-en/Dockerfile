FROM dockerimages/ubuntu-core:trusty

RUN apt-get update \
  && apt-get upgrade -y \
  && apt-get clean -y

RUN apt-get install git python2.7 python2.7-dev python-pip build-essential

RUN cd /opt && rm -Rf src
RUN mkdir /opt/src /opt/run

# Install minteressa.* modules source
RUN cd /opt/src && git clone https://github.com/malberich/pgds-etl-filters.git
RUN cd /opt/src/pgds-etl-filters && git pull origin master

RUN  pip install -r /opt/src/pgds-etl-filters/requirements.txt

RUN cd /opt/src/pgds-etl-filters && python setup.py build && python setup.py install

### modify those script files in order to run yours ###
RUN cp /opt/src/pgds-etl-filters/examples/nearduplicate-en-filter.py /opt/run/nearduplicate-en-filter.py

RUN cd /opt && rm -Rf src

### TODO check for Kafka service
### This script is expected to check for kafka availability ###
CMD ["python", "/opt/run/nearduplicate-en-filter.py"]
