package com.narcasse.canal.demo;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;

import java.net.InetSocketAddress;

/**
 * 通过SingleConnector连接Canal Server(Canal Server只有一台，非HA的情况)
 */
public class SimpleCanalClientDemo extends AbstractCanalClient {

    public SimpleCanalClientDemo(String destination) {
        super(destination);
    }

    public static void main(String args[]) {


        String canalServerHost = "192.168.154.132";
        int canalServerPort = 11111;
        String destination = "example";

        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(canalServerHost, canalServerPort),
                destination,
                "",
                "");

        final SimpleCanalClientDemo client = new SimpleCanalClientDemo(destination);
        client.setConnector(connector);
        client.start();
    }
}
