# -*- coding: utf-8 -*-
"""
Created on Sun Apr 17 21:44:52 2016

@author: Nguyen Duc-Duy

This program read a pickle file that contain the Sparse martrix (https://github.com/composes-toolkit/dissect/blob/master/src/composes/matrix/sparse_matrix.py)
each row is a vector extracted from text headlines. The file the will match with
a file that contain the ObjectID of these document.

If the size of the number of line in the matrix doese not equal to the number of
object IDs, error is raised.

Then, the program do DBScan clustering, the output is IDclusters, each cluster in one line.

@prams
- input_vector_file: the pickle file that contains all vectors of objects
- id_list: list of headline IDs, in the same order with the iput_vector_file
- out_file: text output file, each line is a list of IDs in a the same cluster

@Sample call: python vector_clustering.py top10000.pkl ../data/headIds10000 res.txt
[OUT] 10000
[OUT] Estimated number of clusters: 122
"""

import sys, codecs;
import numpy as np;
import pickle;
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import normalize

# Avariable to save number of data collecting waves
n_wave = 536;
clusters = [];  # List of list list, 
                #  each sublist contains data for a time window
                #   each sub-sublist contain ID of a cluster fond in the twindow

"""
    Check if the compositional parse matrix is a complete ZeroMatrix/Vector
    @Arg: Compositional Sparse matrix to check
    (https://github.com/composes-toolkit/dissect/blob/master/src/composes/matrix/sparse_matrix.py)
    @Out: 
        - True: only contain zero
        - False: not only zero
"""
def isZeroMat(mx):
    return not np.any(mx.mat.toarray())
    
# Load the ID list
id_list=list(codecs.open(sys.argv[2], 'r'));
print "ID list size: ",len(id_list)

# Load the Distributional vector matrix
vec_matrix = pickle.load(open(sys.argv[1], "rb"));
print "Vector matrix size: ",vec_matrix.get_shape();
if (len(id_list)==vec_matrix.get_shape()[0]):
    print "Files size correctly matched!"
    # establish a list of tuple, each contain the possition
    # of the staritng and ending document in the goup.
    # The first document was in 14/10/15 16:43:16Z 
    # The last document was in 06/11/15 0:46:16Z
    # Threfore 536 data collecting waves betweent these two time. So we split
    # data into 536 parts.
    ls=[];
    total = len(id_list);
    n_sam = int(total/n_wave);
    for x in xrange(0,n_wave):    
        if (x==(n_wave-1)):
            ls.append((x*n_sam,(x+1)*n_sam+total%n_wave)); # add remaning item to the last element
        else:
            ls.append((x*n_sam,(x+1)*n_sam));    
    print "Built index pairs list!";

    #Earase the output file
    open(sys.argv[3], 'w').close();  # Erease the file    
    text_out = open(sys.argv[3], 'w'); # Reopen the output for writing
    # Start Clustering
    index = 0;
    for (beg,end) in ls:
        sub_ls = [];
        print "\nEnter cluster ",index;
        # Get submatrix for a period of time
        sub_vec_matrix = vec_matrix[beg:end];
        # run DBSCAN
        print "-> Start clustering from point ",beg, " to ",end,". Sub-matrix size ", sub_vec_matrix.get_shape();
        X = normalize(sub_vec_matrix.mat, norm='l2', axis=1) # Normalize data
#        X = scale(sub_vec_matrix.mat,with_mean=False);
#        print X;        
        db = DBSCAN(eps=1.21, min_samples=2).fit(X);
        labels = db.labels_;    # Get labels
        # Get matrix of datapoint (true value) - outliner (False value)
        core_samples_mask = np.zeros_like(db.labels_, dtype=bool);
        core_samples_mask[db.core_sample_indices_] = True
        outliner_count=0;
        for ele in core_samples_mask:
            if (ele == False): outliner_count+=1;
        # Number of clusters in labels, ignoring noise if present.
        n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
        print "-> Found ", n_clusters_," clusters with ", outliner_count, " outliners";
        # Write the output file
        for lbl in set(labels):
            if lbl==-1: #Skip outliner label
                continue;   
            # get ID of element has this label
            subsub_ls= [id_list[beg:end][x].strip() for x in xrange(0,len(labels)) if (labels[x]==lbl)];
            sub_ls.append(subsub_ls);
#            print indcs;
        index+=1; 
        clusters.append(sub_ls);
    pickle.dump(clusters, text_out); # Write clusters to file
    text_out.close();        
else:
    print "ID list and vector file doese not match in number!"

