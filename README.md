# KafkaSH

A shell interface client for Kafka

## Usage

You need to have a running Kafka cluster. If this cluster is local,
you can use the following lines.

```shell
$ sbt assembly
$ ./bin/kafkash.sh
```

For an external cluster or a cluster on containers, add the option
`--bootstrap.servers` in the parameters and the host and the port of
at least one machine.

## Help

```shell
$ ./bin/kafkash.sh --help
```

