#getcofreqs.py
#USAGE: python getcofreqs [token file]
#EXAMPLE: python getcofreqs tokens.txt
#The token file must contain one token per line.
#_______

import sys

win_one_side=int(sys.argv[2])	#How many words on either side of the target? i.e. 5 corresponds to a 11_word window
win_both_sides=int(win_one_side) * 2
win_full=win_both_sides + 1

cofreqs_dict = {}
freqs_dict = {}

#Shift window by one word
def shiftwindow(w,i,tokens):
	for c in range(win_both_sides):
		w[c]=w[c+1]			#Shift window up to last element, which remains the same (to be replaced by reading new line in lemma file)
	w[win_both_sides]=tokens[i+win_one_side+1]	#Replace last element in window
	return w

#Get cofreqs, and while we're at it, count words
def getcofreqs(w):
  for c in range(win_full):
    if c is not win_one_side:			#We don't count co-occurrences of the word with itself
      co=w[win_one_side]+" "+w[c]
      #print co
      if co in cofreqs_dict:
        cofreqs_dict[co]+=1
      else:
	cofreqs_dict[co] = 1
    else:
      if w[c] in freqs_dict:
        freqs_dict[w[c]]+=1
      else:
        freqs_dict[w[c]]=1
  

#open the token file (one token per line)
filename=sys.argv[1]

#Token array needs padding with non-words at beginning and end
tokens=[]
for c in range(win_one_side):
	tokens.append('#')

lines=open(filename,"r")
for t in lines:
	tokens.append(t.rstrip('\n'))
lines.close()

for c in range(win_one_side):
	tokens.append('#')

#Initialise window
window=tokens[0:win_full]

for i in range(win_one_side,len(tokens)-win_one_side):
	#print "Processing",tokens[i],"..."
	#print window
	getcofreqs(window)
	if i+win_one_side+1<len(tokens):
		window=shiftwindow(window,i,tokens)
		#print window


fcofreqs=open(sys.argv[1]+".sm",'w')
for el in cofreqs_dict:
  fcofreqs.write(el+" "+str(cofreqs_dict[el])+"\n")
fcofreqs.close()

ffreqs=open(sys.argv[1]+".freqs",'w')
for w in sorted(freqs_dict, key=freqs_dict.get, reverse=True):
  freq = freqs_dict[w];
  if (freq>10):
      ffreqs.write(str(freq)+" "+w+"\n")
ffreqs.close()

