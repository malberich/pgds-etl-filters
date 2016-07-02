# pgds-etl-filters

## Requirements

You should have installed and running the [pgds-kafka-backend](https://github.com/rberenguel/pgds-kafka-backend) container's repository, which starts kafka, zookeeper and the twitter sample gatherer script.

For the previous step to work, it may be necessary to add the host "kafka" to the list of hosts in your own machine. This is a workaround until we get a final, full-stack composed version of the sistem as a whole.

### The network isolation workaround

Given that currently the pgds-kafka-backend containers are run by a different docker compose script, there are some issues regarding containers' isolation that may block the etl containers to read from the kafka service.

In order to avoid that, the docker commands below include the flag `--net="host"`, which will use the host (real machine, let's say) network, and thus it'll try to use their ports to start the services.

This can cause some collisions between the containers' services and the hosts' services. So if, for instance you've installed Kafka in your host, and then try to run a kafka container with the flag above, you may experience network binding errors.

## Running a filter for testing purposes

Once you get installed, and provided that you've got a fairly new docker engine version, you can have a first test by running the docker image configured into containers/kafka-test:

```bash
cd containers/kafka-test
docker build -t pgds-filters-kafka-test .
```
It may take a while, as it installs and compiles some python and ubuntu packages. After the system gets configured, the "minteressa" module is installed and the examples/kafka-test.py script is copied into the /opt/run folder. This is the script that will run into the container.

In order to make it run, execute:

```bash
docker run --net="host" -d -t pgds-filters-kafka-test
```

This will start the container. The container gets the tweets to STDOUT, so this means that they can be seen by running docker logs

```bash
docker logs -f $container_id
```
Where $container_id is the identifier of the running container.  This will output all the tweets currently being stored into the current topic.

Of course you can also run docker in foreground mode, so that you can skip the double run. Running the container in foreground will print the tweets to the `STDOUT`:

```bash
docker run --net="host" -t pgds-filters-kafka-test
```
