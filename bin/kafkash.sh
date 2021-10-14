#!/usr/bin/env sh

dir=$(dirname $0)
jar=$dir/../target/scala-2.13/kafkash-assembly-0.1.jar

java -jar $jar $@
