# MVDR
数据库中基于交互式事务的并发控制算法实现与评估

 •一、实现一个哈希索引的关系型内存数据库 ./storage
  •主要工作： 
     （1）实现一个基于行的哈希索引的共享一切架构的关系型内存数据库 
     （2）实现了内存池用于减少系统调用的开销 
     （3）优化内存对齐，提高缓存命中率，避免多线程伪共享的影响 
     
•二、实现模拟TPCC和YCSB测试基准 ./system,./benchmark
  •主要工作： 
     多线程并发模拟初始化TPCC和YCSB的工作负载和事务（如TPCC的payment和new-order的读写插入事务） 

•三、实现本课题的MVDR并发控制协议（基于MVCC、EARLY WRITE VISIBILITY、执行过程中验证、TRANSACTION DEPENDENCY REPAIR、TRANSACTION PRIORITY），
并复现近5年的SIGMOD、VLDB等顶会上发表的最先进的协议(BAMBOO、PLOR、WOUND-WAIT、SILO、WAIT-DIE），在实现的TPCC和YCSB测试基准上评估性能 ./concurrency_control
  •主要工作： 
     实现与评估并发控制协议，在事务高并发处理的过程中保证可序列化的隔离级别 
