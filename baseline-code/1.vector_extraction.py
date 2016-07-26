# -*- coding: utf-8 -*-
"""
Created on Wed Apr 13 22:46:39 2016

@author: Nguyen Duc-Duy

This script build addictive extract text lines into vectors, base on a give distributional model
@ Param: 
    - to_extract_file (each headline/document is in one line)
    - Distributional Semantic space (https://github.com/composes-toolkit/dissect/tree/master/src/composes/semantic_space)
    - Ourput file: file that store vectors
Output format: a matrix, each row the vector of the corresponding text line from the to_extract_file
"""
import sys, codecs,traceback,re;
from composes.utils import io_utils;
from  composes.matrix.sparse_matrix import SparseMatrix;
import numpy as np
from scipy import sparse
import pickle

pattern = re.compile('\W')

# Load the semantic space
my_space = io_utils.load(sys.argv[2]);

# Get the rows in space
key_set = set([]);
for x in my_space.get_row2id():
    key_set.add(x);

# Open the output file
text_out = open(sys.argv[3], 'w')

# Load the headlines
text = codecs.open(sys.argv[1], 'r');
count =0;
line_processed=0;
error_count=0;
# Initialize the sparse mtrix to store result
res_mtx = None;

for line in text:
    line_processed+=1;
    stri = re.sub(pattern, ' ', line.strip());
    stri = re.sub(' +',' ',stri);
    tokens=stri.strip().split(" ");
#    print "\n",tokens;
    try:
        vecs= None;
        try:
            vecs = my_space.get_rows(tokens);
        except:
            new_tokens=[];            
            for tok in tokens: # Filter tokens that on the keyword set of the space
                if (tok in key_set):
                    new_tokens.append(tok);
            vecs = my_space.get_rows(new_tokens); # Now find space of new tokens
            pass;
        shape = vecs.get_shape();
 #       print shape
        if (vecs==None or shape[0]==0):
            print "Error retriving vects for line "+ line;
            error_count+=1;
    #   Fix error that no vector founded. Replace by a zero vector
            buckets = [0] * shape[1];
            mx = np.array(buckets);
            mtx = SparseMatrix(sparse.csr_matrix(mx));
            if (count==0): # Add the zero matrix to the result
                res_mtx=mtx;
            else:
                res_mtx = res_mtx.vstack(mtx);
    #        print mtx;
        else:
            if (shape[0]==1):
                # Write the vector to file
    #            text_out.write(vecs[0]); # Write the first value
    #             print vecs[0]; # Write the first value
    #            for j in xrange(1,shape[1]):# then write the next
    #                text_out.write("\t",vecs[0][j]);
    #            text_out.write("\n");
    #            print vecs[0];
                if (count==0): # Stack the result
                    res_mtx = vecs[0];
                else:
                    res_mtx = res_mtx.vstack(vecs[0]);
                
                count+=1;
            elif shape[0]>1:
                vsum = vecs[0];
                for i in xrange(1,shape[0]):
                    vsum = vsum + vecs[i];
               #print vsum;
    #            for j in xrange(1,shape[1]-1):# then write the next
    #                text_out.write("\t",vsum[j]);
#                print vsum;
    #            text_out.write("\n");
    #            print vsum;
                if (count==0): # Stack the result
                    res_mtx = res_mtx=vsum;
                else:
                    res_mtx = res_mtx.vstack(vsum);
                count+=1;
            if (count%1000==0):
                # write result to file
                #for w in res_mtx:
                #    print w;
                #    text_out.write(w);
                #    text_out.write("\n")
                print "Successed ",count, " in ", line_processed;
                print "--> Current line: ", line;
            #    break;

    except:
            print "ERROR processing line", line, tokens;
#         traceback.print_stack()
            traceback.print_exc();
            error_count+=1;
            pass;
print "Size of vector matrix: ", res_mtx.get_shape(); 
pickle.dump(res_mtx, text_out);
print "Successed ",count, " in ",line_processed," with ", error_count," errors.";
text_out.close();