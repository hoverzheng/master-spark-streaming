# 如何把多个udf作用于同一列数据

## 概述

本文介绍如何把多个udf函数使用到dataframe/dataset的同一列上。

## 使用思路
有时候我们需要在同一列上进行多个函数操作，形成一个函数链。也就是把上一个函数的输出作为下一个函数的输入，把最后的结果作为处理结果。

有多种方式可以实现该功能，这介绍一种函数链的方式，基本思路如下：
1. 把需要对列进行处理的函数放到一个链表中
2. 分别通过函数链上的每个函数来对列数据进行处理
3. 把上一个函数的处理结果作为下一个函数的输入
4. 直到最后一个函数的结果。

## 实现如下：
```
from pyspark.sql import SparkSession
import time

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf, struct
from pyspark.sql.functions import lit
from pyspark.sql.functions import *


class udfs:
    def sum2(self, x):
        return x + 4

    def multi(self, x):
        return x * 2

    def div(self, x):
        return x / 3

fun_list = ["sum2", "multi", "div"]
udfs = udfs()

def my_udf(func_list):
    def all_udf(v):
        r = None
        for f in func_list:
            if r is None:
                r = getattr(udfs, f)(v)
            else:
                r = getattr(udfs, f)(r)
        return r
    return udf(all_udf, IntegerType())


def main():
    spark = SparkSession.builder.enableHiveSupport()\
        .config("hive.exec.dynamic.partition", True)\
        .config("hive.exec.dynamic.partition", True)\
        .config("hive.exec.dynamic.partition.mode", "nonstrict")\
        .appName("Test udf").getOrCreate()

    df = spark.createDataFrame([(101, 1, 16)], ['ID', 'A', 'B'])
    df.show()

    df.withColumn('Result2', my_udf(fun_list)("A")).show()


if __name__ == "__main__":
    main()
```

## 总结
本文介绍了一种通过函数链的方式实现把多个udf函数作用到一个dataframe/dataset列上的方法。
需要说明的是：这种方法未必是最好的方式，但基本的功能还是能够很好的实现。