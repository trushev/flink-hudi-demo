#!/bin/bash
# Start the flink cluster

echo "Configuring Job Manager on this node"
mv /*.jar /usr/local/flink/lib
cp /conf/* /usr/local/flink/conf
sed -i -e "s/jobmanager.rpc.address: localhost/jobmanager.rpc.address: `hostname -i`/g" /usr/local/flink/conf/flink-conf.yaml

/usr/local/flink/bin/jobmanager.sh start #cluster #local
echo "Cluster started."

echo "Config file: " && grep '^[^\n#]' /usr/local/flink/conf/flink-conf.yaml
echo "Sleeping 10 seconds, then start to tail the log file"
sleep 10 && tail -f `ls /usr/local/flink/log/*.log | head -n1`
