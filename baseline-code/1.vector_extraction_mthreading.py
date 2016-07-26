# -*- coding: utf-8 -*-
"""
Created on Wed Apr 13 22:46:39 2016

@author: Nguyen Duc-Duy

This script build addictive extract text lines into vectors, base on a give distributional model
@ Param: 
    - to_extract_file (each headline/document is in one line)
    - Distributional Semantic space (https://github.com/composes-toolkit/dissect/tree/master/src/composes/semantic_space)
    - Ourput file: file that store vectors
    - n_threas: number of thread to run.
Output format: a matrix, each row the vector of the corresponding text line from the to_extract_file

@Example: python 1.vector_extraction_mthreading.py ../data/heads.tagged ../spaces/heads.pkl vectors_limit10_SVD.pkl 4
cd /media/duy/Enlightment/DSproject/scripts
python 1.vector_extraction_mthreading.py ../data/heads.tagged ../spaces/heads3.pkl vectors_limit10_SVD.pkl 4


"""
import sys, codecs,traceback,re, threading;
from composes.utils import io_utils;
from  composes.matrix.sparse_matrix import SparseMatrix;
import numpy as np
from scipy import sparse
import pickle

my_space = None; # The semantic space
space_dim = 1500; # Number of dims of the semantic space
key_set = set([]); # store set of keys of the semantic space

class extract_vectors(threading.Thread):
    def __init__(self, threadName, docs_tks):
        threading.Thread.__init__(self);
        self.threadName = threadName;
        self.docsTks = docs_tks;
        self.size = len(self.docsTks);
        self.resMatrix = None;  # This is the returned sparse matrix of all document
                        # inside the data set. It is in SparseMatrix structure 
        self.nsucceed=0;
    def run(self):
        print "Starting " + self.threadName + ". Data size: " + str(len(self.docsTks));
        success_count=0;
        proccessed_count=0;
        res_max=None;
        # Create a zero vector
        zero_mtx = SparseMatrix(sparse.csr_matrix(np.array([0] * space_dim)));
                    
        try:
            for tokens in self.docsTks: # Go through documents
                vecs = None;
                proccessed_count +=1;
                if (tokens==['']):
                    print ' -> Found an empty line at line '+ str(proccessed_count);
                else:
                    try:
                        vecs = my_space.get_rows(tokens);
                    except:
#                        print "!!!!!"
                        new_tokens=[];            
                        for tok in tokens: # Filter tokens that on the keyword set of the space
                            if (tok in key_set):
                                new_tokens.append(tok);
                        if (len(new_tokens)!=0):
                            vecs = my_space.get_rows(new_tokens);
#                print shape;
                if (vecs==None or vecs.get_shape()[0]==0):
                    print self.threadName, " failed to build vects for line: ", tokens, "because vecs= ", vecs;
                    #   Fix error that no vector founded. Replace by a zero vector
                    if (proccessed_count==1): # Add the zero matrix to the result
                        res_max=zero_mtx;
                    else:
                        res_max = res_max.vstack(zero_mtx);
#                    print mtx;
#                    print mtx;
                else:
#                    print vecs.get_shape();
                    shape = vecs.get_shape();
                    if (shape[0]==1):
                        if (proccessed_count==1): # Stack the result
                            res_max = vecs[0];
                        else:
                            res_max = res_max.vstack(SparseMatrix(vecs[0]));
#                            print vecs[0];
                        success_count+=1;
                    elif shape[0]>1:
                        vsum = vecs[0];
                        for i in xrange(1,shape[0]):
                            vsum = vsum + vecs[i];
                        if (proccessed_count==1): # Stack the result
                            res_max = SparseMatrix(vsum);
                        else:
                            res_max= res_max.vstack(SparseMatrix(vsum));
#                            print vsum;
                        success_count+=1;
#                    print res_max;
#                print "\n",self.threadName,"\t",proccessed_count, "\t",tokens;
                if (res_max==None):
                    print "!!!!!!!!!!!!!!!!!!!!!!!!";
                if (proccessed_count%500==0):
                        print "\n",self.threadName ,"successful processed ",success_count, " in ", proccessed_count;
                        #print "--> Current line: ", line;
#                        break;
            self.resMatrix = res_max
            print "\n@@@@@@@@@@@@@@@@@@@@ ",self.resMatrix.get_shape();
            self.nsucceed = success_count;
        except:
            print "ERR on multi threading technique on ",self.threadName;
            traceback.print_exc();

            
########################################################        
if __name__ == '__main__':
    pattern = re.compile('\W')
    
    # Load the semantic space
    my_space = io_utils.load(sys.argv[2]);
    space_dim = my_space.element_shape[0];
    # Get the rows in space
    for x in my_space.get_row2id():
        key_set.add(x);
    
    # Earase the output file
    open(sys.argv[3], 'w').close() 
    # Open the output file
    text_out = open(sys.argv[3], 'w')
    
    # Load the headlines
    text = codecs.open(sys.argv[1], 'r');
    count =0;
    line_processed=0;
    error_count=0;
    # Initialize the sparse mtrix to store result
    res_mtx = None;
    
    # Get number of threads
    nthreads = int(sys.argv[4]);    
    
    # Split the text into data parts
    data=[]; # Data container. This one is a list whose elements are list of 
            # tokens that will be proccessed by a thred. This token itself 
            #is a list or tokens inside a document. It is a list of list of list
    for i in xrange(0,nthreads):
        data.append([]);
    # Push data to list
    ln_count=0;
    for line in text:
        ln_count+=1;
        stri = re.sub(pattern, ' ', line.strip());
        stri = re.sub(' +',' ',stri);
        tokens=stri.strip().split(" ");
        data[ln_count%nthreads-1].append(tokens);
    print "Splited data to ",len(data), " parts";
#   for ds in data:
#       print len(ds);
        
    # Now start the threads
    threads=[];
    try:
        for i in xrange(0,nthreads):
            new_thread = extract_vectors("Thread-"+str(i),data[i]);
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
    my_space=None;
    
    # Collect results
    final_matrix = threads[nthreads-1].resMatrix;
    total_succecced = threads[nthreads-1].nsucceed;
    total_proccessed = threads[nthreads-1].size;
    print "--> Size of result matrix of ", threads[nthreads-1].threadName ,"is ",threads[nthreads-1].resMatrix.get_shape();
    for i in xrange(nthreads-2,-1,-1):
        final_matrix = final_matrix.vstack(threads[i].resMatrix); # Stack the mats
        total_succecced += threads[i].nsucceed;
        total_proccessed += threads[i].size;     
        print "--> Size of result matrix of ", threads[i].threadName ,"is ",threads[i].resMatrix.get_shape();
    print "Size of vector final matrix: ", final_matrix.get_shape(); 
    pickle.dump(final_matrix, text_out);
    print "Successed ",total_succecced, " in ",total_proccessed," with ", total_proccessed - total_succecced," unsupported document(s) was corrected.";
    text_out.close();