from text_analysis.py import *
import re
import sys
import os
import json
import pandas as pd


def get_rdd(base, input, num_part):
        base_dir = os.path.join(base)
        input_path = os.path.join(input)
        file_name = os.path.join(base_dir, input_path)
        # load data
        rdd = sc.textFile(file_name, num_part)
        rdd_j = rdd.map(json.loads)
        rdd_j.cache()
        return rdd_j

		
def tocsv(data):
	return ','.join(str(d) for d in data)
	

revs = get_rdd('data', 'reviews_musical.json', 4)
revs = get_rdd('data', 'reviews_musical.json', 4)
# revs.take(2)
rev_texts = revs.map(lambda x: (x['asin'], x['reviewText']))

stopfile = os.path.join(baseDir, inputPath, STOPWORDS_PATH)
stopwords = set(sc.textFile(stopfile).collect())
# print 'These are the stopwords: %s' % stopwords

# print tokenize(quickbrownfox) # Should give ['quick', 'brown', ... ]

rev_toks = rev_texts.map(lambda (asin, text): (asin, tokenize(text)))

# totalTokens = countTokens(rev_toks)
# print 'There are %s tokens in the dataset' % totalTokens

# biggestRecord = findBiggestRecord(rev_toks)
# print 'The  record with ID "%s" has the most tokens (%s)' % (biggestRecord[0][0],
                                                             # len(biggestRecord[0][1]))

# print tf(tokenize(quickbrownfox)) # Should give { 'quick': 0.1666 ... }

idfs_rddpair = idfs(rev_toks)
idfs_rddpair = idfs_rddpair.sortBy(lambda x: x[1], ascending=True)
# the file is not so big, use pandas
df = pd.DataFrame(idfs_rddpair.collect())
df.to_csv('data/musical.csv')


# save to hdfs	
# idfs_lines = idfs_rddpair.map(tocsv)
# idfs_lines.saveAsTextFile('data/musical_idfs.csv')

# IDFTokens = idfs_rddpair.takeOrdered(20, lambda s: s[1])
# print IDFTokens




# import matplotlib.pyplot as plt

# small_idf_values = idfsSmall.map(lambda s: s[1]).collect()
# fig = plt.figure(figsize=(8,3))
# plt.hist(small_idf_values, 50, log=True)
# pass



# idfsWeights = idfs_rddpair.collectAsMap()
# tfidsWeights = rev_toks.map(lambda x: tfidf(x[1], idfsWeights

# get all tokens
# toks = rev_toks.flatMap(lambda x: x[1]).distinct()
