import json
import re

# filename
base_dir = os.path.join('data') 
input_path = os.path.join('meta_Musical_Instruments.json') 
file_name = os.path.join(base_dir, input_path)

# load data
num_part = 2
r_prods = sc.textFile(file_name, num_part)
def prepare_str(s):
	s_ = re.sub('\'([a-z]*) ', r"##39##\1 ", s)
	return re.sub('"""*', r"#34#34#", s_).replace('\'', '"')


r_prods_ = r_prods.map(prepare_str)
prods = r_prods_.map(json.loads)
prods.take(7)
prods.cache()
"""
u"{'asin': '0006428320', 'title': 'Six Sonatas For Two Flutes Or Violins, Volume 2 (#4-6)', 'price': 17.95, 'imUrl': 'http://ecx.images-amazon.com/images/I/41EpRmh8MEL._SY300_.jpg', 'salesRank': {'Musical Instruments': 207315}, 'categories': [['Musical Instruments', 'Instrument Accessories', 'General Accessories', 'Sheet Music Folders']]}"
"""
