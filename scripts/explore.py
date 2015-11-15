import json

def load_reviews(file_name, num_part):
	# load data
	reviews = sc.textFile(file_name, num_part)
	reviews = reviews.map(json.loads)
	reviews = reviews.cache() # can't do partition here, must have key!
	return reviews

def load_proucts(file_name, num_part):
	r_prods = sc.textFile(file_name, num_part)
	prods = r_prods.map(json.loads)
	# prods.take(7)
	prods = prods.cache()
	return prods


# TODO: define file_name and num_part here
reviews = load_reviews(file_name, num_part)
# find a product
reviews.filter(lambda x: x[u'asin'] == u'B000TUGSC0').collect() # B000TUGSC0

# numb of reviews of each product
rating = reviews.map(lambda x: (x[u'asin'], [x[u'overall']]))
ratings = rating.reduceByKey(lambda x, y: x + y) # [(u'B000I5562E', [4.0, 1.0, 1.0, 2.0]), (u'B00005K3P6', [5.0]), (u'B000JFEAIO', [5.0, 5.0])]
ave_rating = ratings.map( lambda (x, y): (x, sum(y), len(y)) ) # [(u'B000I5562E', 8.0, 4), (u'B00005K3P6', 5.0, 1), (u'B000JFEAIO', 10.0, 2)]

# top 10 product
# ave_rating.sortBy(lambda (asin, tot, cnt): tot, ascending=False).map(lambda (asin, tot, cnt): (asin, tot / cnt)).take(10) # [(u'B000ULAP4U', 4.703945500993472), (u'B003VWJ2K8', 4.592527472527473), (u'B003VWKPHC', 4.5745477230193385), (u'B001MSS6CS', 4.295774647887324), (u'B00FPPQYXM', 4.706293706293707), (u'B009E3EWPI', 4.6375838926174495), (u'1417030321', 4.448275862068965), (u'B002VA464S', 4.472795497185741), (u'B00063678K', 4.638922888616891), (u'B005CX4GLE', 4.639506172839506)]

# average rating
rating_ave = ave_rating.map(lambda (asin, tot, cnt): tot).reduce(lambda x, y: x + y) / ave_rating.count()

# number of reviews of each product
ave_rating.map(lambda (asin, tot, cnt): cnt).reduce(lambda x, y: x + y) / ave_rating.count()

## number of reviews
num_rev = reviews.count()

# review length ave
rev_len = reviews.map( lambda x: len(x['reviewText']) )
rev_len_ave = rev_len.reduce(lambda x, y: x + y) / num_rev

# TODO: define file_name and num_part here
prods = load_proucts(file_name, num_part)

# number of products
asin = prods.map( lambda x: (x.get('asin'), x.get('price'), x.get('categories')) )
assert(asin.filter(lambda (asin, price, catg): asin == None).count() == 0)
asin.cache()
asin_cnt = asin.map(lambda (asi, pri, cat): (asi, 1)).reduceByKey(lambda x, y: x + y)
num_prod = asin_cnt.count() # 84901

# number of categories
categ = asin.flatMap( lambda (asin, price, catg): catg ).flatMap( lambda x: x ).map( lambda x: (x, 1) )
categ_n = categ.reduceByKey(lambda x, y: x + y)
catg_num = categ_n.count() # 1114

# (catg, 1)
categ_n_dc = categ_n.collectAsMap()
ct_num_j = json.dumps(categ_n_dc)
with open('data/ct_num.json', 'w') as outfile:
    json.dump(ct_num_j, outfile)

# (catg, ave price)
categ_p = asin.map( lambda (asin, price, catg): ([item for sublist in catg for item in sublist], price) ).flatMap( lambda (catg, price): [(ct, (price, 1)) for ct in catg] ).filter(lambda (ct, (price, i)): price != None)
categ_totalp = categ_p.reduceByKey( lambda x, y: (x[0] + y[0], x[1] + y[1]) )
categ_avep = categ_totalp.map(lambda (ct, (p, c)): (ct, p/c))
ct_avep_dc = categ_avep.collectAsMap()
ct_avep_j = json.dumps(ct_avep_dc)
with open('data/ct_avep.json', 'w') as outfile:
    json.dump(ct_avep_j, outfile)


# require join! (review length, catg)
# (asin, catg) + (asin, review length)
asin_catg = asin.map( lambda (asin, price, catg): (asin, [item for sublist in catg for item in sublist], price) ).flatMap( lambda (asin, catg, price): [(asin, ct) for ct in catg] )
asin_revlen = reviews.map( lambda x: (x.get('asin'), len(x.get('reviewText'))))
asin_ct_revl = asin_catg.join(asin_revlen)
ct_revlen = asin_ct_revl.map(lambda (asi, (ct, cnt)): (ct, (cnt, 1))).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).map(lambda (ct, (revl, n)): (ct, revl/n))
ct_revlen_dc = ct_revlen.collectAsMap()
ct_revlen_j = json.dumps(ct_revlen_dc)
with open('data/ct_revlen.json', 'w') as outfile:
    json.dump(ct_revlen_j, outfile)

# require join! (ave rating, catg)
# (asin catg) + (asin, ave rating)
asin_averat = ave_rating.map(lambda (asi, (tot, cnt)): (asi, tot/cnt))
ct_averat = asin_catg.join(asin_averat).map(lambda (asi, (ct, rate)): (ct, (rate, 1))).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).map(lambda (ct, (tot_r, n)): (ct, tot_r/n))
ct_averat_dc = ct_averat.collectAsMap()
ct_averat_j = json.dumps(ct_averat_dc)
with open('data/ct_averat.json', 'w') as outfile:
    json.dump(ct_averat_j, outfile)


