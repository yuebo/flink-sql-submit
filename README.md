Flink SQL Job
----------------
该项目用于使用flink的blink引擎的SQL功能执行sql文件，可以使用于纯sql交互。

可以通过sql定义source和sink等，从而完成etl等的任务。


### 运行
需要下载flink-1.11.1，并且将lib添加至项目的lib环境里面即可运行。

```
单机节点UDF可以添加JVM参数
-Dflink.local.mock=true
```