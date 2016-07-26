"""
EXTRACT KNOWN HUMAN NAME FROM TEXT CORPUS, COUNT THEIR FREQ AND SAVE TO A PICKLE MODEL AS COUNTER
@ Commandline params:
  - The input text file. E.g: ../Data/headlines.txt
  - Number of thread for Multi-threading E.g. 5
  - The output pickle file E.g: model.p
@ IMPORTANT:
  - Consider your computer resource when choosing number of threads
  - The list of firstname and lastname is load from lists/us_firstname.txt, and lists/us_surname.txt. Make sure that you have them in your folder.
"""
from textblob import TextBlob
from textblob import Word
import textblob_aptagger
import codecs
import string
import sys, traceback
from sets import Set
from collections import Counter;
import pickle;
import threading;

# Initialize the tagger
pt = textblob_aptagger.PerceptronTagger()

# Initialize the Counter
name_count= Counter();

# Initialize multi-threading
threads = []

# Load first name set
firstname_set = Set([]);
for line in codecs.open('lists/us_firstname.txt', encoding='utf-8'):
    firstname_set.add(line.strip());
print "Loaded firstname set with " + str(len(firstname_set)) + " elements";

# Load surname set
surname_set = Set([]);
for line in codecs.open('lists/us_surname.txt', encoding='utf-8'):
    surname_set.add(line.strip());
print "Loaded surname set with " + str(len(surname_set)) + " elements";

"""
This will preproccess and break data into smaller data piece, reading for parallel processing
@param:
        - f1: address of the input text file
        - no_threads: number of thread to run
@return: a list, every element is a sublist of string, which are headlines
"""
def textPreprocess(f1, no_threads):
	text = codecs.open(f1, 'r', encoding='utf-8')
#	text_out = open(f2, 'w')
#	names_out = open(f3, 'w')
	res = [];
#     Initialize the return list
	for x in xrange(0,no_threads):
		res.append([]);
     #text=open(sys.argv[1], 'r')
	count = -1;
	for line in text:
		# Add some pre-processing here (boilerplate removal, etc)
		# Remove links, marked by a * in lynx
 		count+=1;
		ctn = line.encode('ascii', 'ignore');
		repls = ('\n' , ''), ('            ' , ''),('^', '');
		txt = reduce(lambda a, kv: a.replace(*kv), repls, ctn);
		txt = txt.rstrip('\n')
		res[count%no_threads].append(txt);
	text.close();
	return res;
  


class myThread (threading.Thread):
	def __init__(self, threadName, textArr):
         threading.Thread.__init__(self)
         self.threadName = threadName
         self.textArr = textArr
         self.res = []

	def run(self):
         print "Starting " + self.threadName
        # Get lock to synchronize threads
         mixed_set = firstname_set.union(surname_set);
         rs=[];
         count = 0;
         for l in self.textArr:
                 count+=1;
                 taggedline = ""
                 try:
        			tags = pt.tag(l)
        #			print tags;
        			if len(tags) > 0:
        				# For POS tag
        				for word in tags:
        					surface = word[0]
        					pos = word[1]
        					try:
        						if pos[0] == 'N' or pos[0] == 'V':
        							tag = Word(surface).lemmatize(
        								pos[0].lower()) + "_" + pos
        						else:
        							tag = Word(surface).lemmatize().lower() + "_" + pos
        						taggedline = taggedline + tag + " "
        					except:
        						taggedline = taggedline + surface + "_" + pos + " "
        				for i in xrange(1,len(tags)):
        #					print tag[i] + ' ' + tag[i-1];
        					fr = tags[i-1][0].encode('utf-8');
        					se = tags[i][0].encode('utf-8');
        #					print type(fr);
        #					print tags[i-1][1],tags[i][1];
#        					if (tags[i-1][1] == "NNP") and (tags[i][1] == "NNP") and (fr in mixed_set) and (se in mixed_set):
        					if (fr in mixed_set) and (se in mixed_set):
        						key=fr+ " " +se;
        						rs.append(key); 						   
                 except:
        			print "ERROR processing line", l
        #                traceback.print_exc()
                   # Free lock to release next thread
         print "Thread "+ self.threadName + " finished processing "+ str(count) +" lines";
         self.res = rs;

################################################################################################ 
if __name__ == '__main__':
	# when executing as script
	print sys.argv;
	nthreads =  int(sys.argv[2]);
	data = textPreprocess(sys.argv[1],nthreads);
	text_out = open("temp.txt", 'w');
	text_out.write(str(data));
	text_out.close();
	# Now call multi threading
	try:
        # Create new threads
         for i in xrange(0,nthreads):
             new_thread = myThread( "Thread-"+str(i),data[i]);
             new_thread.start();
             threads.append(new_thread);

         # Wait for all threads to complete
         for t in threads:
             t.join()
         
         # Put all results together
         name_count = Counter();
         for t in threads:
             name_count.update(t.res);
             t=None;
         print "Exiting Main Thread. Found " + str(len(name_count)) + " names ";
         # Write to pickle file
         pickle.dump( name_count, open( sys.argv[3], "wb" ) )
	except:
         print "Error: unable to run multithreading"
         traceback.print_exc();