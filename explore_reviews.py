import json

# filename
base_dir = os.path.join('data') 
input_path = os.path.join('reviews_Musical_Instruments.json.gz') 
file_name = os.path.join(base_dir, input_path)

# load data
num_part = 2
reviews = sc.textFile(file_name, num_part)
reviews = reviews.map(json.loads)
reviews.cache()

"""
[{u'reviewerID': u'A1YS9MDZP93857', u'asin': u'0006428320', u'reviewerName': u'John Taylor', u'helpful': [0, 0], u'reviewText': u'The portfolio is fine except for the fact that the last movement of sonata #6 is missing. What should one expect?', u'overall': 3.0, u'summary': u'Parts missing', u'unixReviewTime': 1394496000, u'reviewTime': u'03 11, 2014'}]"""

# find a product
reviews.filter(lambda x: x[u'asin'] == u'0006428320').collect()

# numb of reviews of each product
rating = reviews.map(lambda x: (x[u'asin'], [x[u'overall']]))
ratings = rating.reduceByKey(lambda x, y: x + y) # [(u'B000I5562E', [4.0, 1.0, 1.0, 2.0]), (u'B00005K3P6', [5.0]), (u'B000JFEAIO', [5.0, 5.0])]
ave_rating = ratings.map( lambda (x, y): (x, sum(y), len(y)) ) # [(u'B000I5562E', 8.0, 4), (u'B00005K3P6', 5.0, 1), (u'B000JFEAIO', 10.0, 2)]

# top 10 product
ave_rating.sortBy(lambda (asin, tot, cnt): tot, ascending=False).map(lambda (asin, tot, cnt): (asin, tot / cnt)).take(10) # [(u'B000ULAP4U', 4.703945500993472), (u'B003VWJ2K8', 4.592527472527473), (u'B003VWKPHC', 4.5745477230193385), (u'B001MSS6CS', 4.295774647887324), (u'B00FPPQYXM', 4.706293706293707), (u'B009E3EWPI', 4.6375838926174495), (u'1417030321', 4.448275862068965), (u'B002VA464S', 4.472795497185741), (u'B00063678K', 4.638922888616891), (u'B005CX4GLE', 4.639506172839506)]

# average rating
ave_rating.map(lambda (asin, tot, cnt): tot).reduce(lambda x, y: x + y) / ave_rating.count()

# number of reviews of each product
ave_rating.map(lambda (asin, tot, cnt): cnt).reduce(lambda x, y: x + y) / ave_rating.count()

