#### 总体流程
- 构建DAG（调用RDD上的方法）
- DAGScheduler将DAG切分Stage（切分的依据是Shuffle），将Stage中生成的Task以TaskSet的形式给TaskScheduler
- TaskScheduler调度Task（根据资源情况将Task调度到相应的Executor中）
- Executor接收Task，然后将Task丢入到线程池中执行

 ![spark执行流程](https://github.com/lijingxiao/spark/blob/master/%E5%88%87%E5%88%86Stage/spark%E6%80%BB%E4%BD%93%E6%B5%81%E7%A8%8B.png)
