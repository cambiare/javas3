#!/bin/bash


rm /tmp/javas3.log

aws s3 cp s3://dgcs-deploys-dev/tmp/javaS3-0.0.1-SNAPSHOT-jar-with-dependencies.jar .

java \
        -Djavas3.s3_endpoint=http://s3.us-east-1.amazonaws.com/ \
        -Djavas3.io_buffer_size=16384 \
        -Djavas3.buffer_block_size=131072 \
        -Djavas3.read_ahead_size=262144 \
        -Djavas3.pool_member_timeout=1000 \
        -Xmx3g \
        -jar javaS3-0.0.1-SNAPSHOT-jar-with-dependencies.jar gcs-test-033590764901 /dgcs -o,ro,-o,sync_read

#       -Djavas3.s3_endpoint=http://localhost:81/ \
#       -jar javaS3-0.0.1-SNAPSHOT-jar-with-dependencies.jar gcs-test-033590764901 /dgcs -o,ro,-o,max_read=8192,-o,sync_read
