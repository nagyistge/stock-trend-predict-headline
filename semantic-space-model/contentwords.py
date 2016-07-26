import sys
import re

f=open(sys.argv[1],'r')
for l in f:
	l=l.rstrip('\n')
	words=l.split()
	for w in words:
		m=re.search("(.*_.)*",w)
		if m:
			w=m.group(1)
			if "_N" in w or "_V" in w or "_J" in w or "_R" in w:
				print w
f.close()
