from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: Kafka_PySpark.py <broker_list> <topic>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext(appName="Kafka_PySpark")
    ssc = StreamingContext(sc, 10)

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1])
    lines.pprint()

    ssc.start()
    ssc.awaitTermination()




#kafka-console-producer --broker-list quickstart.cloudera:9092 --topic test
#spark2-submit /home/cloudera/Desktop/Kafka_PySpark.py quickstart.cloudera:9092 test
