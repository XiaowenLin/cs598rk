import sys
import json
import gzip

def parse(path):
  g = gzip.open(path, 'r')
  for l in g:
    yield json.dumps(eval(l))

if __name__ == '__main__':
  input = sys.argv[1]
  output = sys.argv[2]
  f = open(output, 'w')
  for l in parse(input):
    f.write(l + '\n')
