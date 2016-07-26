###################################################################################
# Showing the top characteristic contexts for a distribution.
# Requires a model with full (non-reduced) space.
# USAGE: python ./viewdistchars.py Rabbit_N ../spaces/CORE_SS.alice.ppmi.row.dm 20
###################################################################################

import sys

word=sys.argv[1]

dm_filename=sys.argv[2]
cols_filename=dm_filename[0:-2]+"cols"

num_top_chars=sys.argv[3]

def getTopOfDist(columns,numbers,x):
	tmp_dict={}
	c=0
	for i in numbers:
		tmp_dict[columns[c]]=i
		c+=1

	c=0
	for w in sorted(tmp_dict, key=tmp_dict.get, reverse=True):
		print tmp_dict[w], w
		c+=1
		if c > x:
			break

word_vec=[]
cols=[]

cols_file=open(cols_filename,'r')
for l in cols_file:
	l=l.rstrip('\n')
	cols.append(l)
cols_file.close()	


dm_file=open(dm_filename,'r')
for l in dm_file:
	l=l.rstrip('\n')
	fields=l.split()
	w=fields[0]
	if w == word:
		for i in range(1,len(fields)):
			n=float(fields[i])
			word_vec.append(n)
		break
dm_file.close()

getTopOfDist(cols,word_vec,int(num_top_chars))
