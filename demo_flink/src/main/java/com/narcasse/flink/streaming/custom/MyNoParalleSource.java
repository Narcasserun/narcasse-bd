package com.narcasse.flink.streaming.custom;

/**
 * 自定义实现并行度为1的source
 *
 * 模拟产生从1开始的递增数字
 *
 *
 * 注意：
 * SourceFunction 和 SourceContext 都需要指定数据类型，如果不指定，代码运行的时候会报错
 * Caused by: org.apache.flink.api.common.functions.InvalidTypesException:
 * The types of the interface org.apache.flink.streaming.api.functions.source.SourceFunction could not be inferred.
 * Support for synthetic interfaces, lambdas, and generic or raw types is limited at this point
 *
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
