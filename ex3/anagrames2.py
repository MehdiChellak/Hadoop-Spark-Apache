# -*- coding: utf-8 -*-
"""
Created on Mon May 24 22:25:18 2021

@author: ASUS
"""

from pyspark import SparkContext

sc =  SparkContext.getOrCreate()

lines=sc.textFile("./toto.txt")

words=lines.flatMap(lambda x: x.split())

tuples=words.map(lambda x: ("".join(sorted(x)),x))

res=tuples.reduceByKey(lambda a,b: a+" "+b)

output = res.collect()

for (word, count) in output:
    print( count)
    
sc.stop()