#!/usr/bin/python -tt
# -*- coding: utf-8 -*-
######################################
# Author: Nguyen Duc Duy - UNITN
#	FUNCTIONS FOR NEWS PROCESSING
######################################
from sets import Set;
import nltk;
import string;
import time;
import nltk.chunk.named_entity;
import random;
from textblob import Word
import textblob_aptagger
import codecs
from nltk import pos_tag, word_tokenize;
from nltk.corpus import stopwords;
import sys, codecs,traceback,re, threading;
from composes.utils import io_utils;
from composes.semantic_space.space import Space;
from composes.matrix.sparse_matrix import SparseMatrix;
import numpy as np
from scipy import sparse;
from scipy.sparse import vstack
import pickle
from composes.similarity.cos import CosSimilarity
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import normalize
from scipy.sparse import csr_matrix

# Initialise tagger
pt = textblob_aptagger.PerceptronTagger()
# some variables
my_space = None; # The semantic space
space_dim = 1500; # Number of dims of the semantic space
key_set = set([]); # store set of keys of the semantic space

######################################
# CONST DECLARATION
NUM_MIN_NODES = 20;		# Define minimum number of node. If total number of nodes is  < this number, pruning will not be performed
NUM_MAX_NODES = 200; 	# Define maximum number of nodes to keep
NUM_MIN_EDGES = 30;		# Define minimum number of edge. The value could not more than (NUM_MIN_NODES)*(NUM_MIN_NODES-1). If total number of edge is  < this number, pruning will not be performed
NUM_MAX_EDGES = 300; 	# Define maximum number of edge to keep. The value could not more than (NUM_MAX_NODES)*(NUM_MAX_NODES-1)
EDGE_TIME_LIMIT = 20; 	# 10 MINUTES. The range of time from now, that within it, edges will not be eliminated.
NODE_TIME_LIMIT = 20; 	# 10 MINUTES. The range of time from now, that within it, nodes will not be eliminated.
EDGE_FREQ_MIN = 3;	# Minimum frequency that an edge is required to be. Being smaller, it will be eliminated.
NODE_FREQ_MIN = 3;	# Minimum frequency that a node is required to be. Being smaller, it will be eliminated.
MAXIMUM_PRINNING_ITERATIONS=30;	# Maximum iterations to delete node and edges. The higer, the less node and edge will be found.
MIN_WORD_LENGTH = 4;	# Minimum nunber of character of a word, accepted to enter the graph

preferedTags = Set(['NN','NNP','NE']);
exceptionsWords = ['a','the','aboard','about','above','across','after','against','along','amid','among','anti','around','as','at','before','behind','below','beneath','beside','besides','between','beyond','but','by','concerning','considering','despite','down','during','except','excepting','excluding','following','for','from','in','inside','into','like','minus','near','of','off','on','onto','opposite','outside','over','past','per','plus','regarding','round','save','since','than','through','to','toward','towards','under','underneath','unlike','until','up','upon','versus','via','with','within','without'];
regex = re.compile('[%s]' % re.escape(string.punctuation)) #see documentation here: http://docs.python.org/2/library/string.html
stop_words = set(stopwords.words('english'));
stop_words.update(['.', ',', '"', "'", '?', '!', ':', ';', '(', ')', '[', ']', '{', '}','n\'t']);
stop_words.update(exceptionsWords);
"""
######################################
#### From a corpus of Json, it, it return a list of all text after prcessed
# Input: a LIST of Json
# Output: a list of clneanned documents, filtered
"""
def TextPreprocess(inpJSls):
    print " - Started Text preprocessing...";
#    start_time = time.time();
    #print "[--!--] Enter time point 0 - Begin Mashing:" + str(time.time() - start_time);
    text_lines = [];
    res=[]
    count = 0;
	#print "[--!--] Enter time point 1 - start processing corpus:" + str(time.time() - start_time);
    for js in inpJSls:
        #print "[--!--] Enter time point 1.1 - Start preprocessing" + str(time.time() - start_time);
        count += 1;
		#jl = json.loads(js.replace('\n',''));
		#docls.append(jl);
		# Tokenize
		#text = nltk.word_tokenize(jl['content']);
        ctn = js['content'].encode('ascii', 'ignore');
        repls = ('\n' , ''), ('            ' , ''),(".", ""),(",", ""),("-", " ");
        text = reduce(lambda a, kv: a.replace(*kv), repls, ctn);
        text_lines.append(text);
    # Now procees the lines
    print " - Found ", len(text_lines), " lines in total ", count, " raw headlines";
    for l in text_lines:
#        print l;
        count+=1;
        taggedline = ""
        l = l.rstrip('\n')
        try:
            tags = pt.tag(l)
            if len(tags) > 1:
                # Run NLTK tag as reference
                nltk_tags= nltk.pos_tag(word_tokenize(l));
                if len(nltk_tags) == len(tags):
                    #Pair-wise compare the tag
                    diff_count=0; # count number of different tags from the both 2 tagger
                    sens="";    # new string, that contain corrected word
                    for i in xrange(0,len(tags)):
                        if tags[i][1] == nltk_tags[i][1]:
                            sens = sens + ' ' + tags[i][0];
                        else: # the two tagger did not agreed
                            sens = sens + ' ' + tags[i][0].lower(); # Lowwer case it
                            diff_count+=1;
                    if (diff_count>1):
#                        print sens, l;
                        tags = pt.tag(sens);
                # for POS tag
                for i in xrange(0,len(tags)): 
                    word =   tags[i]                  
                    surface = re.sub(r'^[^a-zA-Z_]', "", word[0]);
                    if ((surface.lower() in stop_words) or len(surface)<3):
                        continue;
                    pos = word[1]
    #				print word
                    try:
                        if pos[0] == 'N' or pos[0] == 'V':
                            tag = Word(surface).lemmatize(
                                pos[0].lower());
                        else:
                            tag = Word(surface).lemmatize().lower()
                            
                        taggedline = taggedline + tag + " "
                    except:
                        taggedline = taggedline + surface + " "
				# for Human name searching
    				
        except:
            print "ERROR processing line", l
        # Collect the line
        if len(taggedline)>0:
            res.append(taggedline);
    return res;
    
"""
######################################
#### Load the semantic space
# Input: the semantic space
"""
def Load_Semantic_Space(space_file):
    # Load the semantic space
    global my_space;
    my_space = io_utils.load(space_file);
    #Normalize the space
#    my_space = my_space.apply(RowNormalization())    
    
    global space_dim;
    space_dim = my_space.element_shape[0];
    # Get the rows in space
    
    keyset = set([]);
    for x in my_space.get_row2id():
        keyset.add(x);
    global key_set;
    key_set=keyset;
"""
######################################
#### A thread worker that extract features
# Output: nothing (load data directly)
# Input: 
#   - threadName: name of the thread
#   - docs_tks: list of documentes, to be processced
# Output: 
#   - self.resMatrix: result extracted matrix
#   - self.proceedRows: list of headlines that was proccessed. Note that, the
#   order of the elements is the row order of the matrix
"""
class Extract_vectors(threading.Thread):
    def __init__(self, threadName, docs_tks):
        threading.Thread.__init__(self);
        self.threadName = threadName;
        self.docsTks = docs_tks;
#        print self.docsTks;
        self.size = len(self.docsTks);
        self.resMatrix = None;  # The returned sparse matrix of all document
                        # inside the data set. It is in SparseMatrix structure
        self.proceedRows=[];
        self.nsucceed=0;
        print "      Initializing " + self.threadName + \
            ". Raw data size: " + str(self.size)+ \
                ".";
    def run(self):
        print "      Starting " + self.threadName + ". Data size: " + \
                                           str(len(self.docsTks));
        success_count=0;
        proccessed_count=0;
        res_max=None;
        processed_docs=[];
                    
        try:
            for tokens in self.docsTks: # Go through documents
                vecs = None;
                proccessed_count +=1;
                if (tokens==['']):
                    print '       -> Found an empty line at line '+ \
                                        str(proccessed_count);
                else:
                    try:
                        vecs = my_space.get_rows(tokens);
                    except:
#                        print "!!!!!"
                        #traceback.print_exc();
                        new_tokens=[];            
                        for tok in tokens: # Filter tokens that on the keyword
                                           # set of the space
                            if (tok in key_set):
                                new_tokens.append(tok);
                        if (len(new_tokens)!=0):
                            vecs = my_space.get_rows(new_tokens);
#                print shape;
                if (vecs==None or vecs.get_shape()[0]==0):
                    print "      <!>",self.threadName, " failed to build vects for line: "\
                                                , tokens, "because vecs= ", vecs;
#                    #   Fix error that no vector founded. Replace by a zero vector
#                    if (proccessed_count==1): # Add the zero matrix to the result
#                        res_max=zero_mtx;
#                    else:
#                        res_max = res_max.vstack(zero_mtx);
#                    print mtx;
#                    print mtx;
                else:
#                    print vecs.get_shape();
                    shape = vecs.get_shape();
                    processed_docs.append(' '.join([str(x) for x in tokens]));
                    if (shape[0]==1):
                        if (success_count==0): # Stack the result
                            res_max = SparseMatrix(vecs[0]);
                        else:
                            res_max = res_max.vstack(SparseMatrix(vecs[0]));
#                            print vecs[0];
                        success_count+=1;
                    elif shape[0]>1:
                        vsum = vecs[0];
                        for i in xrange(1,shape[0]):
                            vsum = vsum + vecs[i];
                        if (success_count==0): # Stack the result
                            res_max = SparseMatrix(vsum);
                        else:
                            res_max= res_max.vstack(SparseMatrix(vsum));
#                            print vsum;
                        success_count+=1;
#                    print res_max;
#                    self.proceedRows = [' '.join([str(x) for x in tokens])] + self.proceedRows;
#                print "\n",self.threadName,"\t",proccessed_count, "\t",tokens;
                if (res_max==None):
                    print "!!!!!!!!!!!!!!!!!!!!!!!!";
                if (proccessed_count%500==0):
                        print "\n      ",self.threadName ,"successful processed ",\
                                        success_count, " in ", proccessed_count;
                        #print "--> Current line: ", line;
#                        break;
            self.resMatrix = res_max
            print "      <i>",self.threadName ," came to result size: ",self.resMatrix.get_shape();
            self.nsucceed = success_count;   
#            processed_docs.reverse();
            self.proceedRows = processed_docs;
#            print self.proceedRows;
        except:
            print "ERR on multi threading technique on ",self.threadName;
            traceback.print_exc();
            
"""
######################################
# From a LIST of text (each headline is a member of list), it extracts feature
# and return a Sparse semantic space, where rows is a headline, the matrix is
# the vectors of the text
# Input: 
#    - List of documents
#    - (!) Semantic space -> load from global variable
#    - Number of threads
# Output: the semantic space contain the IDs and vectors for extracted text
"""
def TextExtraction(text,nthreads):
    pattern = re.compile('\W')   
    # Split the text into data parts
    data=[]; # Data container. This one is a list whose elements are list of 
            # tokens that will be proccessed by a thred. This token itself 
            #is a list or tokens inside a document. It is a list of list of list
    for i in xrange(0,nthreads):
        data.append([]);
    # Push data to list
    ln_count=0;
    text2 = set([]);
    for ln in set(text):
        ln = ln.replace(".", "").replace(",", "").replace("-", " ");
        ln = re.sub(pattern, ' ', ln.strip());
        ln = re.sub(' +',' ',ln);
        text2.add(ln.strip(" \n"));
#    print text2;
    for line in text2:
        ln_count+=1;
        tokens=line.strip().split(" ");
        data[ln_count%nthreads-1].append(tokens);
    print "      Splited data to ",len(data), " parts";
#   for ds in data:
#       print len(ds);
        
    # Now start the threads
    threads=[];
    try:
        for i in xrange(0,nthreads):
            new_thread = Extract_vectors("Thread-"+str(i),data[i]);
            new_thread.start();
            threads.append(new_thread);
    except:
        print "Error: unable to run multithreading";
        traceback.print_exc();
        
     # Wait for all threads to complete
    for t in threads:
        t.join()
        
    # Release data
    data=None;
    #my_space=None;
    
    # Collect results
    final_matrix = threads[0].resMatrix;
    rows = threads[0].proceedRows; 
    total_succecced = threads[nthreads-1].nsucceed;
    total_proccessed = threads[nthreads-1].size;
    print "      --> Size of result matrix of ",threads[nthreads-1].threadName\
          ,"is ",threads[nthreads-1].resMatrix.get_shape();
    for i in xrange(1,nthreads,1):
        final_matrix = final_matrix.vstack(threads[i].resMatrix); # Stack mats
        total_succecced += threads[i].nsucceed;
        total_proccessed += threads[i].size;     
        print "      --> Size of result matrix of ", threads[i].threadName ,\
              "is ",threads[i].resMatrix.get_shape();
        rows.extend(threads[i].proceedRows);  
    print "      Size of vector final matrix: ", final_matrix.get_shape(); 
    print "      Successed ",total_succecced, " in ",total_proccessed," with "\
          , total_proccessed - total_succecced,\
          " unsupported document(s) was corrected.";
    cols = [i for i in xrange(0,space_dim)];
    try:
        print "      --> Starting space build. Size to matrix " + \
                str(final_matrix.get_shape()) + \
                ". Size of rows " + str(len(rows)) +\
                ". Size of columns " + str(len(cols));
        heads_space = Space(final_matrix,rows,cols,None,None,[],None);
        print "          Toy neigbors: ",heads_space.get_neighbours(rows[0], 3\
              ,CosSimilarity());
 #       print heads_space.get_id2row();
    except:
        print "/!\ Error building semantic space!"
        traceback.print_exc();
   # Now write to file
#    space_out = open(sys.argv[3]+".space", 'w')
    return heads_space;
#    pickle.dump(heads_space, space_out);
#    space_out.close(); # Close space file
    print "      Saved the space file!"
    
"""
######################################
# Run clustering on a co-occureance matrix
# return a label list, which is the assigned number as cluster ID
# Input: 
#    - Cooccorence matrix, as Numpy sparse matrix 
(can be extract from DSSpace: my_space.cooccurrence_matrix)
     - ID list: list of row ID (probaly the headline itself)
# Output: a list, which contain sublists, each sublist contain ID of documents
whic is in the same cluster
"""
def RunDBSCAN(cooc_matrix,id_list):
    print "      - Start clustering on ", cooc_matrix.get_shape(), \
          " data block.";
    rs = [];    
    X = normalize(cooc_matrix.mat, norm='l2', axis=1) # Normalize data
    db = DBSCAN(eps=1.15, min_samples=3).fit(X);
    labels = db.labels_;    # Get labels
    # Get matrix of datapoint (true value) - outliner (False value)
    core_samples_mask = np.zeros_like(db.labels_, dtype=bool);
    core_samples_mask[db.core_sample_indices_] = True
    outliner_count=0;
    for ele in core_samples_mask:
        if (ele == False): outliner_count+=1;
    # Number of clusters in labels, ignoring noise if present.
    n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
    print "      -> Found ", n_clusters_," clusters with ", outliner_count, \
          " outliners";
    for lbl in set(labels):
        if lbl==-1: #Skip outliner label
            continue;   
        # get ID of element has this label
        subsub_ls= [id_list[x] for x in xrange(0,len(labels)) if (labels[x]==lbl)];
        rs.append(subsub_ls);
    return rs;

"""
######################################
# Compute MAX vector from matrix. How it work: Get max value of each column.
# The result is another sparse matrix (1,n_dims) to represent the whole matrix
# Input: 
#    - a Numpy sparse matrix
# Output: a (1,n_dims) matrix
"""
def MaxZipping(matrix):
    trans = matrix.transpose();
    maxls=[];
    for i in xrange(0,matrix.get_shape()[1]):
        maxls.append(trans[i].max());
    return csr_matrix(maxls);

"""
######################################
# Compute MEAN vector from matrix. How it work: Get max value of each column.
# The result is another sparse matrix (1,n_dims) to represent the whole matrix
# Input: 
#    - a Numpy sparse matrix
# Output: a (1,n_dims) matrix
"""
def MeanZipping(matrix):
    trans = matrix.transpose();
    meanls=[];
    for i in xrange(0,matrix.get_shape()[1]):
        meanls.append(trans[i].mean());
    return csr_matrix(meanls)

"""
######################################
# Compute the vector for the whole cluster
# 
# Input: 
#    - the DISTRIBUTED semantic space specifict to that time window
#    - document cluster list, each element is a list contain ID/headers that is
# in the same cluster
# Output: a (1,n_dims) matrix (vector) represent the whole cluster
"""
def ClusterVectorExtraction(dsspace,clusters):
    print "      - Start Cluster vector extraction.";
    count=0;
    cluster_matrix=None;
    # Get cluster's headlines
    for cluster in clusters:
        cluster_vector = csr_matrix(dsspace.get_rows(cluster).mat.sum(axis=0));
        if (count == 0):
            cluster_matrix = cluster_vector;
        else:
           cluster_matrix = vstack([cluster_matrix,cluster_vector]); 
        count+=1;
    result_vector = csr_matrix(cluster_matrix.sum(axis=0))    
    print "      --> The result vector: ",result_vector.toarray();
    return result_vector;