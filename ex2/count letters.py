# -*- coding: utf-8 -*-
"""
Created on Mon May 24 21:34:18 2021

@author: ASUS
"""


from pyspark import SparkContext

sc =  SparkContext.getOrCreate()

lines=sc.textFile('input.txt')

words=lines.flatMap(lambda x: x.split())

tuples=words.map(lambda x: (x[0], 1))

res=tuples.reduceByKey(lambda a,b: a+b)

# save the counts to output
res.saveAsTextFile("D:/workspace/spark/output/tata3")

output = res.collect()

for (word, count) in output:
    print(word, count)
    
sc.stop()