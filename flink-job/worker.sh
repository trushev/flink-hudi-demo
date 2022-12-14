# Start the flink task manager (slave)
echo "Configuring Task Manager on this node"
FLINK_NUM_TASK_SLOTS=${FLINK_NUM_TASK_SLOTS:-`grep -c ^processor /proc/cpuinfo`}
FLINK_MASTER_PORT_6123_TCP_ADDR=`host ${FLINK_MASTER_PORT_6123_TCP_ADDR} | grep "has address" | awk '{print $4}'`

cp /conf/* /usr/local/flink/conf
mv /flink-hudi-demo.jar /usr/local/flink
mv /*.jar /usr/local/flink/lib

sed -i -e "s/jobmanager.rpc.address: localhost/jobmanager.rpc.address: ${FLINK_MASTER_PORT_6123_TCP_ADDR}/g" /usr/local/flink/conf/flink-conf.yaml
sed -i -e "s/taskmanager.numberOfTaskSlots: [0-9]\+/taskmanager.numberOfTaskSlots: ${FLINK_NUM_TASK_SLOTS}/g" /usr/local/flink/conf/flink-conf.yaml

echo "Starting Task Manager"
/usr/local/flink/bin/taskmanager.sh start

echo "Config file: " && grep '^[^\n#]' /usr/local/flink/conf/flink-conf.yaml
echo "Sleeping 10 seconds"
sleep 10

echo "Submitting job"
/usr/local/flink/bin/flink run -d /usr/local/flink/flink-hudi-demo.jar --checkpointing
