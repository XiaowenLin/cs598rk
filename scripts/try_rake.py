from scripts.rake import *
import json
rake = Rake('data/SmartStoplist.txt')
def get_rdd(base, input, num_part):
            base_dir = os.path.join(base)
            input_path = os.path.join(input)
            file_name = os.path.join(base_dir, input_path)
            # load data
            rdd = sc.textFile(file_name, num_part)
            rdd_j = rdd.map(json.loads)
            rdd_j.cache()
            return rdd_j
revs = get_rdd('data', 'reviews_electronics500.json', 4)
rev_texts = revs.map(lambda x: (x['asin'], x['reviewText']))
# aggregate by asin
rev_agg_texts = rev_texts.map(lambda (asin, text): (asin, [text])).reduceByKey(lambda x, y: x + y)
rev_agg_rake = rev_agg_texts.map( lambda (asin, texts): (asin, rake.run(' '.join(texts))) )
rev_agg_rake10 = rev_agg_rake.map( lambda (asin, rakes): (asin, rakes[:10]) )
