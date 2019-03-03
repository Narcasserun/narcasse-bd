出错信息
--------------
ava.util.concurrent.ExecutionException: 
org.apache.kafka.common.errors.TimeoutException: 
Expiring 1 record(s) for gp-topic-0: 32166 ms 
has passed since batch creation plus linger time


出错原因
--------------
1、通过host访问broker
2、可能是advertised.listeners设置错误：https://www.jianshu.com/p/71b295e1df4f
