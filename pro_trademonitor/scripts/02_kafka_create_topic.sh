#!/usr/bin/env bash

# topic gp_ods_1
bin/kafka-topics.sh --delete --zookeeper dev-bg-02:2181/kafka  --if-exists  --topic gp_ods_1
bin/kafka-topics.sh --create --zookeeper dev-bg-02:2181/kafka  --if-not-exists --replication-factor 1 --partitions 2 --topic gp_ods_1

# topic gp_ods_2
bin/kafka-topics.sh --delete --zookeeper dev-bg-02:2181/kafka  --if-exists  --topic gp_ods_2
bin/kafka-topics.sh --create --zookeeper dev-bg-02:2181/kafka  --if-not-exists --replication-factor 1 --partitions 2 --topic gp_ods_2

# topic gp_ods_3
bin/kafka-topics.sh --delete --zookeeper dev-bg-02:2181/kafka  --if-exists  --topic gp_ods_3
bin/kafka-topics.sh --create --zookeeper dev-bg-02:2181/kafka  --if-not-exists --replication-factor 1 --partitions 2 --topic gp_ods_3

# topic gp_ods_4
bin/kafka-topics.sh --delete --zookeeper dev-bg-02:2181/kafka  --if-exists  --topic gp_ods_4
bin/kafka-topics.sh --create --zookeeper dev-bg-02:2181/kafka  --if-not-exists --replication-factor 1 --partitions 2 --topic gp_ods_4

# topic gp_metric
bin/kafka-topics.sh --delete --zookeeper dev-bg-02:2181/kafka  --if-exists  --topic gp_metric
bin/kafka-topics.sh --create --zookeeper dev-bg-02:2181/kafka  --if-not-exists --replication-factor 1 --partitions 2 --topic gp_metric


# 验证topic是否创建成功

bin/kafka-topics.sh --describe --zookeeper dev-bg-02:2181/kafka   --topic gp_ods_1
bin/kafka-topics.sh --describe --zookeeper dev-bg-02:2181/kafka   --topic gp_ods_2
bin/kafka-topics.sh --describe --zookeeper dev-bg-02:2181/kafka   --topic gp_ods_3
bin/kafka-topics.sh --describe --zookeeper dev-bg-02:2181/kafka   --topic gp_ods_4
bin/kafka-topics.sh --describe --zookeeper dev-bg-02:2181/kafka   --topic gp_metric
