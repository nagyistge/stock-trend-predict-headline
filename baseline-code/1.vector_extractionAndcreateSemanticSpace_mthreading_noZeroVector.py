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
python 1.vector_extractionAndcreateSemanticSpace_mthreading.py ../data/heads.tagged ../spaces/heads.pkl vectors_limit10_SVD.pkl 4


"""
import sys, codecs,traceback,re, threading;
from composes.utils import io_utils;
from composes.semantic_space.space import Space;
from  composes.matrix.sparse_matrix import SparseMatrix;
import numpy as np
from scipy import sparse
import pickle
#from composes.transformation.scaling.row_normalization import RowNormalization

my_space = None; # The semantic space
space_dim = 1500; # Number of dims of the semantic space
key_set = set([]); # store set of keys of the semantic space

class extract_vectors(threading.Thread):
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
        print "Initializing " + self.threadName + \
            ". Raw data size: " + str(self.size)+ \
                ".";
    def run(self):
        print "Starting " + self.threadName + ". Data size: " + \
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
                    print ' -> Found an empty line at line '+ \
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
                    print self.threadName, " failed to build vects for line: "\
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
                        print "\n",self.threadName ,"successful processed ",\
                                        success_count, " in ", proccessed_count;
                        #print "--> Current line: ", line;
#                        break;
            self.resMatrix = res_max
            print "\n@@@@@@@@@@@@@@@@@@@@ ",self.resMatrix.get_shape();
            self.nsucceed = success_count;   
#            processed_docs.reverse();
            self.proceedRows = processed_docs;
#            print self.proceedRows;
        except:
            print "ERR on multi threading technique on ",self.threadName;
            traceback.print_exc();

            
########################################################        
if __name__ == '__main__':
    pattern = re.compile('\W')
    
    # Load the semantic space
    my_space = io_utils.load(sys.argv[2]);
    
    #Normalize the space
#    my_space = my_space.apply(RowNormalization())    
    
    space_dim = my_space.element_shape[0];
    # Get the rows in space
    for x in my_space.get_row2id():
        key_set.add(x);

    # Earase the output file
    open(sys.argv[3], 'w').close() 

    
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
    final_matrix = threads[0].resMatrix;
    rows = threads[0].proceedRows; 
    total_succecced = threads[nthreads-1].nsucceed;
    total_proccessed = threads[nthreads-1].size;
    print "--> Size of result matrix of ", threads[nthreads-1].threadName ,"is ",threads[nthreads-1].resMatrix.get_shape();
    for i in xrange(1,nthreads,1):
        final_matrix = final_matrix.vstack(threads[i].resMatrix); # Stack the mats
        total_succecced += threads[i].nsucceed;
        total_proccessed += threads[i].size;     
        print "--> Size of result matrix of ", threads[i].threadName ,"is ",threads[i].resMatrix.get_shape();
        rows.extend(threads[i].proceedRows);  
    print "Size of vector final matrix: ", final_matrix.get_shape(); 
    # Open the output vector file
    text_out = open(sys.argv[3], 'w')
    pickle.dump(final_matrix, text_out);
    print "Successed ",total_succecced, " in ",total_proccessed," with ", total_proccessed - total_succecced," unsupported document(s) was corrected.";
    text_out.close(); # Close vector file
    # Write row file        
#    print 'rows=', rows;
    rows_f = open(sys.argv[3]+".rows", 'w');    
    for rws in rows:
        rows_f.write(rws+"\n");
    # Now build the semantic space
        # Load the headlines column
    #cols = codecs.open(sys.argv[5], 'r');  #Load the columns
    cols = [i for i in xrange(0,space_dim)];
    try:
        print "Starting space build. Size to matrix " + \
                str(final_matrix.get_shape()) + \
                ". Size of rows " + str(len(rows)) +\
                ". Size of columns " + str(len(cols));
        heads_space = Space(final_matrix,rows,cols,None,None,[],None);
    except:
        print "/!\ Error building semantic space!"
        traceback.print_exc();
   # Now write to file
    space_out = open(sys.argv[3]+".space", 'w')
    pickle.dump(heads_space, space_out);
    space_out.close(); # Close space file
    print "Wrote the space file!"
