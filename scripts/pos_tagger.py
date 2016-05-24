from pyspark import SparkConf
from pyspark import SparkContext

sc = SparkContext()

data = sc.textFile('/home/cs598rk/tenlines.json')

import nltk

words = data.flatMap(lambda x: nltk.word_tokenize(x))
print words.take(10)


pos_word = words.map(lambda x: nltk.pos_tag([x]))
print pos_word.take(5)
