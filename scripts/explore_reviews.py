import json

# filename
base_dir = os.path.join('data') 
input_path = os.path.join('meta_musical.json') 
file_name = os.path.join(base_dir, input_path)

# load data
num_part = 2
r_prods = sc.textFile(file_name, num_part)
prods = r_prods.map(json.loads)
prods.take(7)
prods.cache()
"""
u"{'asin': '0006428320', 'title': 'Six Sonatas For Two Flutes Or Violins, Volume 2 (#4-6)', 'price': 17.95, 'imUrl': 'http://ecx.images-amazon.com/images/I/41EpRmh8MEL._SY300_.jpg', 'salesRank': {'Musical Instruments': 207315}, 'categories': [['Musical Instruments', 'Instrument Accessories', 'General Accessories', 'Sheet Music Folders']]}"
"""

# number of products
asin = prods.map( lambda x: (x.get('asin'), x.get('price'), x.get('categories')) )
assert(asin.filter(lambda (asin, price, catg): asin == None).count() == 0)
asin_cnt = asin.map(lambda (asi, pri, cat): (asi, 1)).reduceByKey(lambda x, y: x + y)
num_prod = asin_cnt.count() # 84901

# number of categories
categ = asin.flatMap( lambda (asin, price, catg): catg ).flatMap( lambda x: x ).map( lambda x: (x, 1) )
categ_n = categ.reduceByKey(lambda x, y: x + y)
catg_num = categ_n.count() # 1114

# (catg, 1)
categ_n

# (catg, ave price)
categ_p = asin.map( lambda (asin, price, catg): ([item for sublist in catg for item in sublist], price) ).flatMap( lambda (catg, price): [(ct, (price, 1)) for ct in catg] ).filter(lambda (ct, (price, i)): price != None)
categ_totalp = categ_p.reduceByKey( lambda x, y: (x[0] + y[0], x[1] + y[1]) )
categ_avep = categ_totalp.map(lambda (ct, (p, c)): (ct, p/c))

