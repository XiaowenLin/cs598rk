from scripts.rake import *
import json
# get review texts aggregated by asin id
def get_rdd(base, input, num_part):
                    base_dir = os.path.join(base)
                    input_path = os.path.join(input)
                    file_name = os.path.join(base_dir, input_path)
                    # load data
                    rdd = sc.textFile(file_name, num_part)
                    rdd_j = rdd.map(json.loads)
                    rdd_j.cache()
                    return rdd_j
num_part = 4
revs = get_rdd('data', 'reviews_electronics.json', num_part)
revs.filter(lambda x: len(x) == 0).collect()
a = revs.take(1)
a
a[0]
a = a[0]
a
a[0]
a['asin']
?a.pop
a.pop('asin')
a
revs_ = revs.filter( lambda x: (x.pop('asin'), x) )
revs_.take(1)
revs_ = revs.filter( lambda x: (a = x.pop('asin'), x) )
revs_.take(2)
num_part = 4
revs = get_rdd('data', 'reviews_electronics.json', num_part)
rev_texts = revs.map(lambda x: (x['asin'], x['reviewText']))
rev_agg_texts = rev_texts.map(lambda (asin, text): (asin, [text])).reduceByKey(lambda x, y: x + y)
rev_agg_texts.cache()
prods = get_rdd('data', 'meta_electronics.json', num_part)
prods.take(2)
ls data/
rake = Rake('data/MergedStopList.txt')
rake.run('The Kelby Training DVD Mastering Blend Modes in Adobe Photoshop CS5 with Corey Barker is a useful tool for becoming familiar with the use of blend modes in Adobe Photoshop. For those who are serious about mastering all that Photoshop has to offer, mastering blend modes is just as important as mastering layers.In this DVD tutorial, seasoned expert Corey Barker explores the function of blend modes in a variety of scenarios such as image restoration, sharpening, adjustments, special effects and much more. Since every project scenario is different, Corey encourages you to experiment with these blend modes by giving you the skills and confidence you need.')
rake.run('Kelby Training DVD: Adobe Photoshop CS5 Crash Course By Matt Kloskowski')
# number of products
asin = prods.map( lambda x: (x.get('asin'), x.get('price'), x.get('categories')) )
assert(asin.filter(lambda (asin, price, catg): asin == None).count() == 0)
asin_cnt = asin.map(lambda (asi, pri, cat): (asi, 1)).reduceByKey(lambda x, y: x + y)
num_prod = asin_cnt.count() # 84901
# number of categories
categ = asin.flatMap( lambda (asin, price, catg): catg ).flatMap( lambda x: x ).map( lambda x: (x, 1) )
categ_n = categ.reduceByKey(lambda x, y: x + y)
categ_n.take(1)
categ_n.take(10)
prods = get_rdd('data', 'meta_electronics.json', num_part)
asin = prods.map( lambda x: (x.get('asin'), x.get('price'), x.get('categories')) )
assert(asin.filter(lambda (asin, price, catg): asin == None).count() == 0)
asin_cnt = asin.map(lambda (asi, pri, cat): (asi, 1)).reduceByKey(lambda x, y: x + y)
num_prod = asin_cnt.count() # 84901
# number of categories
categ = asin.flatMap( lambda (asin, price, catg): catg ).flatMap( lambda x: x ).map( lambda x: (x, 1) )
categ_n = categ.reduceByKey(lambda x, y: x + y)
categ_n.take(10)
categ_n.take(100)
categ_df = categ_n.toDF)_
categ_df = categ_n.toDF()
categ_df
import pandas as pd
categ_df = pd.DataFrame(categ_df.collect())
categ_df.head()
?categ_df.sort
categ_df.head()
categ_df_sort = categ_df.sort(categ_df['1'])
categ_df.columns
categ_df_sort = categ_df.sort(categ_df[1])
categ_df[1]
categ_df
categ_df_sort = categ_df.sort([1])
?categ_df.sort_values
categ_df_sort = categ_df.sort_values('1')
categ_df_sort = categ_df.sort_values(1)
categ_df_sort
categ_df_sort.to_csv('data/processed/electronics_categories.csv')
categ_df_sort.to_csv('data/processed/electronics_categories.csv')
categ_df_sort
categ.take(3)
prods.take(3)
categ = prods.map( lambda x: (x.get('asin'), x.get('categories')) )
?categ.flatMapValues
categ_ = categ.flapMapValues(lambda x: x)
categ_ = categ.flatMapValues(lambda x: x)
categ_.take(1)
computers = categ_.filter( lambda (asin, cats): 'Computers & Accessories' in cats )
computers.take(2)
computers = computers.partitionBy(num_part)
prods = prods.partitionBy(num_part)
?prods.join
computers.take(2)
computers = computers.cache()
computers
prods.take(2)
prods.take(2)
prods = get_rdd('data', 'meta_electronics.json', num_part)
prods.take(2)
prods = prods.map( lambda x: (x['asin'], x) )
prods.take(2)
prods = prods.map(lambda (asin, d): (asin, d.pop('asin')))
prods.take(2)
prods = get_rdd('data', 'meta_electronics.json', num_part)
prods_ = prods.map( lambda x: (x.pop('asin'), x) )
prods_.take(3)
prods_ = prods_.partitionBy(num_part)
prods_.take(3)
prods_.cache()
prods_ = prods_.join(computers)
prods_.take(1)
prods_.count()
# 234558, match previous counting for computers
rev_agg_texts.take(1)
rev_agg_texts.cache()
itesm = rev_agg_texts.join(prods_)
items = rev_agg_texts.join(prods_)
items.take(2)
items.cache()
items.map(lambda x: x.pop('categories'))
items.take(1)
%history -f scripts/try_computer.py
