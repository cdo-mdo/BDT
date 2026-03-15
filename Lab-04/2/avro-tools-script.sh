#!/bin/sh

echo "run avro-tools"
echo avro-tools tojson output/part-r-00000.avro
avro-tools tojson output/part-r-00000.avro
echo avro-tools tojson --pretty output/part-r-00000.avro
avro-tools tojson --pretty output/part-r-00000.avro

