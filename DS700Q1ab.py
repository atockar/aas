import os
import re
folder = '../../medline_data'

authsCS = set()
authsCI = set()
fPatt = re.compile('\<ForeName\>(.*)\<\/ForeName\>')
lPatt = re.compile('\<LastName\>(.*)\<\/LastName\>')

for xml in os.listdir(folder):
	with open(folder+'/'+xml, 'r') as f:
		lines = f.read().split("\n")
		for line in lines:
			lName = lPatt.match(line)
			if lName:
				name = lName.group(0).strip()
			fName = fPatt.match(line)
			if fName:
				name = fName.group(0).strip() + " " + name
				authsCS.add(name)
				authsCI.add(name.upper())

# Check answer to Q1a (count distinct authors)
print len(authsCS)	# 485220

# Check answer to Q1b (count distinct authors case insensitive)
print len(authsCI)	# 476500