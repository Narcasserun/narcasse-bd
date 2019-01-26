package com.narcasse.flink.streaming.custom;


/**
 * 自定义实现一个支持并行度的source
 *
 */

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.concurrent.TimeUnit;

public class MyParalleSource implements ParallelSourceFunction<Long> {
    private boolean isRunnning = true;
    private long count=1l;
    /**
     * 主要的方法
     * 启动一个source
     * 大部分情况下，都需要在这个run方法中实现一个循环，这样就可以循环产生数据了
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunnning){
            ctx.collect(count);
            count++;
            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Override
    public void cancel() {
        this.isRunnning = false;
    }
}
