from text_analysis.py import *
import re
import sys
import os
import json


def get_rdd(base, input, num_part):
        base_dir = os.path.join(base)
        input_path = os.path.join(input)
        file_name = os.path.join(base_dir, input_path)
        # load data
        rdd = sc.textFile(file_name, num_part)
        rdd_j = rdd.map(json.loads)
        rdd_j.cache()
        return rdd_j


revs = get_rdd('data', 'reviews_musical.json', 4)
import json
revs = get_rdd('data', 'reviews_musical.json', 4)
revs.take(2)
rev_texts = revs.map(lambda x: x['reviewText'])

quickbrownfox = 'A quick brown fox jumps over the lazy dog.'


split_regex = r'W+'
print simpleTokenize(quickbrownfox, split_regex) # Should give ['a', 'quick', 'brown', ... ]







stopfile = os.path.join(baseDir, inputPath, STOPWORDS_PATH)
stopwords = set(sc.textFile(stopfile).collect())
print 'These are the stopwords: %s' % stopwords

print tokenize(quickbrownfox) # Should give ['quick', 'brown', ... ]







googleRecToToken = googleSmall.map(lambda (id, string): (id, tokenize(string)))

totalTokens = countTokens(googleRecToToken)
print 'There are %s tokens in the dataset' % totalTokens





biggestRecordGoogle = findBiggestRecord(googleRecToToken)
print 'The Google record with ID "%s" has the most tokens (%s)' % (biggestRecordGoogle[0][0],
                                                                   len(biggestRecordGoogle[0][1]))





print tf(tokenize(quickbrownfox)) # Should give { 'quick': 0.1666 ... }







idfsSmall = idfs(googleRecToToken)
uniqueTokenCount = idfsSmall.count()




smallIDFTokens = idfsSmall.takeOrdered(11, lambda s: s[1])
print smallIDFTokens




import matplotlib.pyplot as plt

small_idf_values = idfsSmall.map(lambda s: s[1]).collect()
fig = plt.figure(figsize=(8,3))
plt.hist(small_idf_values, 50, log=True)
pass




recb000hkgj8k = googleRecToToken.collect()[0][1]
idfsSmallWeights = idfsSmall.collectAsMap()
rec_b000hkgj8k_weights = tfidf(recb000hkgj8k, idfsSmallWeights)

print 'Amazon record "b000hkgj8k" has tokens and weights:n%s' % rec_b000hkgj8k_weights






