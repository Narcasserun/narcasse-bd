package com.narcasse.flink.streaming.sink;

/*
发送数据到redis
*/

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class StreamDemoToRedis {


    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = env.socketTextStream("ubuntu1", 9999);


        SingleOutputStreamOperator<Tuple2<String, String>> stream = streamSource.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return new Tuple2<>("1_words", value);
            }
        });
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("ubuntu1").setPort(6379).setPassword("123456").build();

        RedisSink<Tuple2<String,String>> redisSink = new RedisSink<>(config,new MyRedisMapper());

        DataStreamSink<Tuple2<String, String>> streamSink = stream.addSink(redisSink);


        try {
            env.execute("StreamDemoToRedis");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    public static class MyRedisMapper implements RedisMapper<Tuple2<String,String>>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }

        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }
    }

}
