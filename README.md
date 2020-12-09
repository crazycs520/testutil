# testutil

## build

```shell
make
```

## usage

#### bench

```shell
bin/testutil bench --sql "select * from t where a=1"
```
# case test introduction

## write conflict

### command: 

```shell
testutil case write-conflict --concurrency 100 --interval 5 --probability 100
```

- concurrency: 指定并发连接数。
- interval: 指定打印相关执行信息的间隔时间，默认值是 1 秒。
- probability: 指定冲突的概率，默认值是 100, 表示冲突的概率是 1/100。

### introduction

表 t 的定义如下：

```sql
CREATE TABLE `t` (
  `id` int(11) NOT NULL,
  `name` varchar(10) DEFAULT NULL,
  `count` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
);
```

多个连接并行执行以下 SQL：

```sql
insert into t values (@a,'aaa', 1) on duplicate key update count=count+1;  
```
SQL 中的 @a 是随机值，取值范围是 [0, probability), @a 所在的 column 上有 unique index。

定期打印以上查询打印慢日志信息以及冲突的错误数量。


