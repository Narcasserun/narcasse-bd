package com.narcasse.canal.demo;


import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;

/**
 * 通过ClusterConnector连接Canal Server集群(Canal Server有多台，HA架构)
 */
public class ClusterCanalClientDemo extends AbstractCanalClient {

    public ClusterCanalClientDemo(String destination) {
        super(destination);
    }

    public static void main(String args[]) {
        String destination = "example";

        // 基于固定canal server的地址，建立链接，其中一台server发生crash，可以支持failover

        // CanalConnector connector = CanalConnectors.newClusterConnector(
        // Arrays.asList(new InetSocketAddress(
        // AddressUtils.getHostIp(),
        // 11111)),
        // "stability_test", "", "");

        // 基于zookeeper动态获取canal server的地址，建立链接，其中一台server发生crash，可以支持failover
        CanalConnector connector = CanalConnectors.newClusterConnector("127.0.0.1:2181", destination, "", "");

        final ClusterCanalClientDemo clientTest = new ClusterCanalClientDemo(destination);
        clientTest.setConnector(connector);
        clientTest.start();

    }
}
