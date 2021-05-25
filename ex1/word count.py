# -*- coding: utf-8 -*-
"""
Created on Mon May 24 21:17:33 2021

@author: ASUS
"""


from pyspark import SparkContext
from operator import add

sc =  SparkContext.getOrCreate();
lines = sc.textFile("input.txt")  # this is an RDD
# counts is an rdd is of the form (word, count)
counts = lines.flatMap(lambda x: [(w.lower(), 1) for w in x.split()])
counts = counts.reduceByKey(add)

output = counts.collect()

for (word, count) in output:
    print(word, count)

sc.stop()
