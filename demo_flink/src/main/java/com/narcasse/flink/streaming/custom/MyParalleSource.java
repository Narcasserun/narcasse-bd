package com.narcasse.flink.streaming.custom;

/*

 */

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.concurrent.TimeUnit;

public class MyParalleSource implements ParallelSourceFunction<Long> {
    private boolean isRunnning = true;
    private long count=1l;

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
