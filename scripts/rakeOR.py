from scripts.rake import *
from nltk.tag import pos_tag
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
import json
# get review texts aggregated by asin id
def get_rdd(base, input, num_part):
        base_dir = os.path.join(base)
        input_path = os.path.join(input)
        file_name = os.path.join(base_dir, input_path)
        rdd = sc.textFile(file_name, num_part)
        rdd_j = rdd.map(json.loads)
        rdd_j.cache()
        return rdd_j

num_part = 4
revs = get_rdd('data', 'reviews_electronics5000.json', num_part)
rev_texts = revs.map(lambda x: (x['asin'], x['reviewText']))
rev_agg_texts = rev_texts.map(lambda (asin, text): (asin, [text])).reduceByKey(lambda x, y: x + y)
rev_agg_texts.cache()
# (asin, ([review], (d_prod, [category])) )
rake = Rake('data/MergedStopList.txt')
wordnet_lemmatizer = WordNetLemmatizer()

rev_agg = rev_agg_texts.map(lambda (asin, revs): (asin, ' '.join(revs)))
tok_agg = rev_agg.map(lambda (asin, rev): (asin, pos_tag(word_tokenize(rev))))
lemma_agg = tok_agg.map(lambda (asin, toks): (asin, [(wordnet_lemmatizer.lemmatize(pair[0]),pair[1]) for pair in toks]))
