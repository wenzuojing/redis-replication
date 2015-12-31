# redis-replication

redis 数据同步，工作原理跟redis主从同步是一样的。

## 应用场景

- 同步redis数据到其他存储，如MySQL、MongoDB、LevelDB、Elasticsearch等等
- 订阅redis数据变更事件
- 数据备份/镜像
- ...

## 使用例子

```

        Replication replication = new Replication("172.16.1.152", 6379, new Pipeline() {

            @Override
            public void process(String[] cmd) {
                System.out.println(Arrays.toString(cmd));
                //同步redis数据MySQL
                ....
            }
        });

        replication.doReplication();

```
