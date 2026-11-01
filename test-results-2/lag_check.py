import os
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, _ConsumerGroupTopicPartitions

bs = os.environ["KAFKA_BOOTSTRAP"]
adm = AdminClient({"bootstrap.servers": bs})
for g in ["fraud-scoring-consumer", "notification-sender-consumer", "audit-reconciliation-consumer"]:
    res = adm.list_consumer_group_offsets([_ConsumerGroupTopicPartitions(g)])[g].result()
    probe = Consumer({"bootstrap.servers": bs, "group.id": "lag-probe"})
    for tp in res.topic_partitions:
        if tp.topic.startswith("digital."):
            _, end = probe.get_watermark_offsets(TopicPartition(tp.topic, tp.partition), timeout=10)
            lag = end - tp.offset if tp.offset >= 0 else "no-commit"
            print(g, tp.topic, tp.partition, "committed=", tp.offset, "end=", end, "lag=", lag)
    probe.close()
