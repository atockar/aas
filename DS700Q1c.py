## List of authors with differing cases

from collections import defaultdict

# Iterate over all distinct case sensitive authors, cast to upper, then store in count array

authCounts = defaultdict(int)

with open('check/authsCS.txt','r') as f:
	lines = f.read().split("\n")
	for line in lines:
		authCounts[line.strip().upper()] += 1

with open('check/q1c_check.txt','w') as g:
	for auth in authCounts:
		if authCounts[auth] > 1:
			g.write(auth + "\n")