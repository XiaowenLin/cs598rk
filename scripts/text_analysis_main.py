from text_analysis.py import *
import re
import sys
import os
from test_helper import Test
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
%history
%history -f text_analysis_main.py
