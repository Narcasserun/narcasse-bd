package com.narcasse.flink.streaming.custom;

/*

 */

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

public class MyNoParalleSource implements SourceFunction<Long> {

    private boolean isRunnning = true;
    private long count = 1l ;

    /**
     * 启动一个source的主要方法，
     * 大部分的情况下只需要在这个run里面循环就行了，这样就可以循环产生数据了
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
    /**
     * 取消一个cancel的时候会调用的方法
     *
     */
    @Override
    public void cancel() {
        this.isRunnning = false;
    }
}
