# -*- coding: utf-8 -*-
"""
Created on Mon May 24 22:47:46 2021

@author: ASUS
"""


from pyspark import SparkContext
from pandas import DataFrame

sc =  SparkContext.getOrCreate()

lines=sc.textFile("../plindromes.txt")

words=lines.flatMap(lambda x: x.split())

tuples=words.map(lambda x: (x == "".join(reversed(x)),x))

res=tuples.reduceByKey(lambda a,b: a+" - "+b)

output = res.collect()

for (word, count) in output:
    if word == True:
        print(count)
sc.stop()