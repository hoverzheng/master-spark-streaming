## 深入浅出RDD Persist和Cache

前面已经介绍了persist和cache的基本原理。那么RDD是如何进行persist和cache操作的呢？

### Persist RDD如何执行

从实现层面，persist()函数只是对RDD的storageLevel（存储级别）进行了设置，函数内部并没有执行任何数据缓存的动作。那么，当设置完RDD的storageLevel后，RDD的缓存动作是如何触发的，又是如何实现的呢？

首先，在抽象类RDD中，定义了一个没有实现的函数compute，该函数用来计算RDD的一个分区。该函数由各种类型的RDD（RDD的子类）来实现，因为每种类型的RDD计算方式不同。该函数的定义如下：

```scala
  def compute(split: Partition, context: TaskContext): Iterator[T]
```

另外，在RDD抽象类中还提供的一个迭代器函数：

```scala
  final def iterator(split: Partition, context: TaskContext): Iterator[T] = {
    // 若设置了RDD的存储级别：storageLevel，从缓存中读取或则重新计算
    if (storageLevel != StorageLevel.NONE) {
      getOrCompute(split, context)
    } else {
      // 默认是存储级别是NONE，调用下面的函数来重新计算RDD，或从checkpoint读取数据
      computeOrReadCheckpoint(split, context)
    }
  }
```

### getOrCompute函数

getOrCompute函数会根据StorageLevel先从本地的存储中读取分区数据，若读取失败，则调用compute函数来计算分区数据。

```scala
/**
 * Compute an RDD partition or read it from a checkpoint if the RDD is checkpointing.
 */
private[spark] def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[T] =
{
  // 若checkpoint中存在该分区，则直接读取该分区的数据，调用compute函数计算该分区数据
  if (isCheckpointedAndMaterialized) {
    firstParent[T].iterator(split, context)
  } else {
    compute(split, context)
  }
}
```

该函数用来读取或计算RDD的分区。该函数的执行步骤如下：

1. 先从本地获取分区数据，若本地不存在该分区的数据则会通过传输服务从远端节点获取分区数据。
2. 若从本地或远端没有获取到数据，则调用computeOrReadCheckpoint函数从checkpoint数据保存目录获取数据，若没有进行checkpoint则调用compute来计算。

### Persist实战

#### 数据准备

```scala
scala> val rdd = sc.parallelize(Seq("Hello World", "Just doit"))
scala> val mapres = rdd.map(x => x.split(" "))
scala> mapres.collect()
```

#### cache和persist实战

```scala
scala> mapres.getStorageLevel.description
res1: String = Serialized 1x Replicated

scala> mapres.getStorageLevel.toString
res2: String = StorageLevel(1 replicas)

# 进行persist操作，其实就是进行cache操作
scala> mapres.persist()
res3: mapres.type = MapPartitionsRDD[1] at map at <console>:25

# 再次查看rdd的状态，可以看到其状态改了，存储级别是memory了
scala> mapres.getStorageLevel.toString
res4: String = StorageLevel(memory, deserialized, 1 replicas)

# 修改StoreageLevel
scala> import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel

# 修改存储级别时报错了。
# 可见，在同一个SparkContext下，若设置了RDD的存储级别，则无法修改
scala> mapres.persist(StorageLevel.MEMORY_AND_DISK)
java.lang.UnsupportedOperationException: Cannot change storage level of an RDD after it was already assigned a level
  at org.apache.spark.rdd.RDD.persist(RDD.scala:170)
  at org.apache.spark.rdd.RDD.persist(RDD.scala:195)
  ... 49 elided
```

### 小结



