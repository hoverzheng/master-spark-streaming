# 使用Spark SQL的Join

### 概述

join操作在进行数据处理时非常常见，而spark支持多种join类型。本文对spark中多种Join类型进行说明，并对不同join的使用场景进行了介绍和举例说明。

### 使用join操作的注意事项

- 在两个数据集比较的列有唯一值，使用默认join(inner join)会有较好的性能，但要注意：两个数据集中不匹配的key值的数据行将会被丢掉，另外，当比较的列有重复值时，会进行排列组合操作，此时可能会衍生出大量的数据，很可能会产生shuffle。
- 当两个数据集有重复的key时，执行join操作时可能会让数据膨胀，此时，最好先对数据集进行distinct或combineByKey操作来减少key值的数量。也可以使用cogroup来处理重复key，避免产生全局排列组合方式来产生数据。在合并时使用智能分区，可以防止join过程中产生二次shuffle。
- 若有的key只存在在两个数据集中的一个中，就有可能产生数据丢失，此时使用outer join更加安全。这样可以保证把数据全都保存在其中的一个数据集中，然后再进行过滤。
- 如果在进行join之前，可以过滤一些key，就尽量过滤掉，这样可以减少join过程中的数据传输量。
- 注意：join是非常耗资源的操作，所以在进行join之前，应该尽量过滤掉不需要的数据。

## join类型说明

#### inner join(默认类型)

默认的join类型，在Spark SQL中的操作是：t1.Id = t2.Id。

- 适用场景
  - 两个数据集比较的列的值唯一(去重)，可以有较好的性能
- 注意事项
  - 当两个数据集不匹配时，数据将会被丢掉
  - 当比较的列有重复值时，会进行排列组合操作，此时可能会衍生出大量的数据，很可能会产生shuffle。

#### outer join

- 使用场景
  - 需要保留全部数据，而不光是匹配上的数据
- 注意事项
  - 使用outer join产生的数据量可能会比较大
  - 使用outer join后往往会再进行过滤操作
  - 需要根据具体的数据集情况来选择把数据保存在left还是right的数据集中

## join操作及其说明

### 测试数据集

本文使用以下两个测试数据集，如下：

- left.csv

```
id,addr
1,a1
2,a2
3,a3
4,a4
10,
11,
12,
```

- right.csv

```
id,name
1,n1
2,n2
3,n3
4,n4
5,n5
6,n6
7,n7
8,n8
9,n9
```

- 通过pyspark进行加载和测试

```
left = spark.read.option("header","true").csv("left.csv")
right = spark.read.option("header","true").csv("right.csv")
```

通过以上代码得到两个dataframe。如下：

```
>>> left.show()
+---+----+
| id|addr|
+---+----+
|  1|  a1|
|  2|  a2|
|  3|  a3|
|  4|  a4|
| 10|null|
| 11|null|
| 12|null|
+---+----+

>>> right.show()
+---+----+
| id|name|
+---+----+
|  1|  n1|
|  2|  n2|
|  3|  n3|
|  4|  n4|
|  5|  n5|
|  6|  n6|
|  7|  n7|
|  8|  n8|
|  9|  n9|
+---+----+
```

通过这两个dataframe来理解一下的join操作。

### inner join(默认)

```
>>> r = left.join(right, left.id== right.id, "inner")
>>> r.show()
+---+----+---+----+
| id|addr| id|name|
+---+----+---+----+
|  1|  a1|  1|  n1|
|  2|  a2|  2|  n2|
|  3|  a3|  3|  n3|
|  4|  a4|  4|  n4|
+---+----+---+----+
# 或
>>> r  = left.join(right, left.id == right.id)
>>> r.show()
+---+----+---+----+
| id|addr| id|name|
+---+----+---+----+
|  1|  a1|  1|  n1|
|  2|  a2|  2|  n2|
|  3|  a3|  3|  n3|
|  4|  a4|  4|  n4|
+---+----+---+----+
```

从以上操作可以看出，默认的==号操作就是inner join。

#### 要点：

- 从以上的结果可以看出，凡是id不相等的列都被过滤掉了，所以要注意inner join会把不匹配的数据丢掉。

### left outer join

#### 要点：

- 当id不匹配时，left outer join会保留左边数据集的全部数据。
- 这样：左边数据集的数据不会丢失。

```
>>> r = left.join(right, left.id== right.id, "leftouter")
>>> r.show()
+---+----+----+----+
| id|addr|  id|name|
+---+----+----+----+
|  1|  a1|   1|  n1|
|  2|  a2|   2|  n2|
|  3|  a3|   3|  n3|
|  4|  a4|   4|  n4|
| 10|null|null|null|
| 11|null|null|null|
| 12|null|null|null|
+---+----+----+----+
```

#### 用途：

- 当处理两张全量表时，有时候会找出两张全量表中的新增id，此时就可以使用left outer join来计算:

  ```
  select * from A left outer join  B on (A.id=B.id) where right.id is null 
  ```

### left semi join和left anti join

#### 要点：

- left semi join只会保留左边数据集的字段。而且需要满足：左边数据集的id必须在右边数据集中存在。

- left anti join也只会保留左边数据集的字段。而且要满足条件：左边数据集的id不能在右边数据集中。

```
# left semi join
>>> r = left.join(right, left.id== right.id, "leftsemi")
>>> r.show()
+---+----+
| id|addr|
+---+----+
|  1|  a1|
|  2|  a2|
|  3|  a3|
|  4|  a4|
+---+----+

# left anti join,只保留左边字段，而且id不能在右边数据集存在
>>> r = left.join(right, left.id== right.id, "leftanti")
>>> r.show()
+---+----+
| id|addr|
+---+----+
| 10|null|
| 11|null|
| 12|null|
+---+----+
```

### right join

right join会保留右边数据集的全部记录。包括两部分：(1)和左边数据集匹配的记录，(2)和左边数据集不匹配的记录，此时左边的列会用空填充。

```
>>> r = left.join(right, left.id== right.id, "right")
>>> r.show()
+----+----+---+----+
|  id|addr| id|name|
+----+----+---+----+
|   1|  a1|  1|  n1|
|   2|  a2|  2|  n2|
|   3|  a3|  3|  n3|
|   4|  a4|  4|  n4|
|null|null|  5|  n5|
|null|null|  6|  n6|
|null|null|  7|  n7|
|null|null|  8|  n8|
|null|null|  9|  n9|
+----+----+---+----+
```

从以上结果中可以看出，右边数据集的数据被全部保留下来了，这样保证了右边数据集不丢失。

### right semi join和right anti join

这两个操作和left semi join以及left anti join相似，不同的是：这两个操作只保留右边数据集的字段。

(这里不再累述。)

### full outer join

该outer操作是left outer join和right outer join结果的合集。要注意，该操作可能会非常耗时，需要谨慎使用。

```
>>> r = left.join(right, left.id== right.id, "fullouter")
>>> r.show()
+----+----+----+----+                                                           
|  id|addr|  id|name|
+----+----+----+----+
|null|null|   7|  n7|
|  11|null|null|null|
|   3|  a3|   3|  n3|
|null|null|   8|  n8|
|null|null|   5|  n5|
|null|null|   6|  n6|
|null|null|   9|  n9|
|   1|  a1|   1|  n1|
|  10|null|null|null|
|   4|  a4|   4|  n4|
|  12|null|null|null|
|   2|  a2|   2|  n2|
+----+----+----+----+
```

### 在join时广播小数据集

当一个表很小而另一个表很大时，可以使用broadcast方式来进行join。

```
>>> r = left.join(F.broadcast(right), left.id== right.id, "rightouter")
>>> r.show()
+----+----+---+----+
|  id|addr| id|name|
+----+----+---+----+
|   1|  a1|  1|  n1|
|   2|  a2|  2|  n2|
|   3|  a3|  3|  n3|
|   4|  a4|  4|  n4|
|null|null|  5|  n5|
|null|null|  6|  n6|
|null|null|  7|  n7|
|null|null|  8|  n8|
|null|null|  9|  n9|
+----+----+---+----+
```
### 总结
join操作是比较消耗性能的，在进行join操作时尽可能进行去重，并去掉不需要的数据。另外，可以通过广播小数据集等方式来加速join的过程。

## 参考

- http://kirillpavlov.com/blog/2016/04/23/beyond-traditional-join-with-apache-spark/
- https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-joins.html