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

多个连接并行执行以下 SQL：

```sql
insert into t values (@a,'aaa', 1) on duplicate key update count=count+1;  
```

SQL 中的 @a 是随机值，取值范围是 [0, probability), @a 所在的 column 上有 unique index。

定期打印以上查询打印慢日志信息以及冲突的错误数量。


