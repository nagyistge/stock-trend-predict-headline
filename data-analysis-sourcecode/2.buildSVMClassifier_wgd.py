#!/usr/bin/python -tt
# -*- coding: utf-8 -*-
"""
######################################
# Author: Nguyen Duc Duy - UNITN
# Project: stock data prediction from newslines
#         the system predict the increase/decrease of DOWN JOHN base on
#         the media behaviors on newlines
######################################

Created on Mon May 16 19:57:42 2016

Usage: 
python 2.buildSVMClassifier_wgd.py dataset.vectors dataset.labels report.csv
python 2.buildSVMClassifier_wgd.py testset.maxmean.cos.vectors testset.labels maxmean_report_gd.csv
python 2.buildSVMClassifier_wgd.py testset.sumsum.cos.vectors testset.labels sumsum_report.csv

Parameters:
    - dataset_vector_mat: dataset vector matrix (pickle file), in numpy sparse
    matrix format.
    - dataset_label_mat: dataset label matrix (picke file), in numpy sparse
    matrix formatncol
    - [OUT] Report file name (text)
    - [OUT] The sklearn SVM classifier (picke file) name
    (see http://scikit-learn.org/stable/modules/svm.html)
    
Description:  this is the second python script for the project
    The program open the dataset from two files: dataset_vector_mat (which is
    the vector file and dataset_label_mat (which is the label file). Each of 
    these file contains an object in Numpy Sparse Matrix format ( see
    http://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.csr_matrix
    .html#scipy.sparse.csr_matrix). 
    
    After convert to array, it feed the data to a sklearn SVM, to build 
    a classifier. The code also run cross-validation on the data set.
    By default, it run 5-fold cross-validation
"""
from __future__ import division
import sys, pickle,traceback,math;
import numpy as np;
from sklearn import cross_validation,svm,preprocessing;
from scipy.sparse import csr_matrix, vstack;
from scipy.spatial.distance import cosine;


#################################  FUNCTIONS   ################################
print(__doc__);

"""
######################################
#### Softmax function to the vector (in numpy array format)
# Input: input numpy array
# Output: result numpy array
"""
def softmax_vec(x):
    e_x = np.exp(x - np.max(x))
    out = e_x / e_x.sum()
    return out
"""
######################################
#### Softmax function to the NUMPY SPARSE MATRIX, do softmax row by row
# Input: input numpy sparse matrix
# Output: soft sparse matrix
"""
def softmax_mat_row(matx):
    out=None;
    for i in xrange(0,matx.shape[0]):
        row = csr_matrix(softmax_vec(matx[i]));
        if (i==0):
            out= row;
        else:
            out = vstack([out,row]);
    return out

"""
######################################
#### Softmax function to the NUMPY SPARSE MATRIX, do softmax to the WHOLE matrix
# Input: input numpy sparse matrix
# Output: soft sparse matrix
"""
def softmax_mat_full(matx):
    shp = matx.shape;
    out = softmax_vec(matx.reshape((1,shp[0]*shp[1])));
    out = out.reshape(shp);
    return out

"""
######################################
#### Cosine similarity data pre-proccesscing
# Input: The matrix as NUMPY ARRAY, each row is a vector document
# Output: The preproccessed numpy matrix
  How it works?
  Since comparing vectors mostly use the angle between vector (consine 
  similarity), I transform every vector into the consine of its angles to the
  axis.
"""

def consineTransform(matx):
    nnow,ncol = matx.shape;
    identify_matx= np.identity(ncol); # initialize the identity matrix 
    res_matx=None;
    for i in xrange(0,nnow): # For each row (vector in the matrix)
        # print res_matx;
        vect = matx[i];
        res_vec = np.zeros(ncol); # Init the zero result vector
        for j in xrange(0,ncol): # For each dimmension of the 
            res_vec[j] = cosine(vect,identify_matx[j]); 
        # Save result
        if (res_matx is not None):
          #  print res_matx.shape,"____",res_vec.shape;
            res_matx = vstack((res_matx,res_vec)); # Append to result matrix
        else:
            res_matx = res_vec.reshape((1,ncol));

    # print "ZZZZZZZZZZZZZZZZZZZZZZ",res_matx.shape;
    return res_matx


################################# MAIN PROGRAM ################################
if __name__ == '__main__':
    # Load dataset
    dataset_vectors_mat= pickle.load(open(sys.argv[1],'rb'));
    print " - Loaded Dataset vectors. Shape: ",dataset_vectors_mat.get_shape();
    dataset_labels_mat= pickle.load(open(sys.argv[2],'rb'));
    print " - Loaded Dataset labels. Shape: ",dataset_labels_mat.get_shape();
    
    # Convert data to standard Scikit-Learn dataset format
    dataset_vectors_mat = dataset_vectors_mat.toarray();
    # Scale the vectors
    dataset_vectors_mat_scaled = preprocessing.scale(dataset_vectors_mat);
    print " - Scaled data: ",dataset_vectors_mat_scaled.shape;
    # Normalize vectors
    dataset_vectors_mat_normal = preprocessing.normalize(dataset_vectors_mat, norm='l2');
    print " - Normalized data: ",dataset_vectors_mat_normal.shape;
    #  Binarization vectors
    dataset_vectors_mat_binarization = preprocessing.binarize(dataset_vectors_mat);    
    print " - Binarized data: ",dataset_vectors_mat_binarization.shape;
    # Softmax rows of data
    dataset_vectors_mat_rowsoft= softmax_mat_row(dataset_vectors_mat);
    print " - Softmax ROW data: ",dataset_vectors_mat_rowsoft.shape;
    # Softmax full of data
    dataset_vectors_mat_fullsoft= softmax_mat_full(dataset_vectors_mat); 
    print " - Softmax FULL data: ",dataset_vectors_mat_fullsoft.shape;
    # Cosine similarity transformation
    dataset_vectors_mat_cos= consineTransform(dataset_vectors_mat);    
    print " - Cosinized data: ",dataset_vectors_mat_fullsoft.shape;
    
    dataset_labels_mat = dataset_labels_mat.toarray().flatten();
    print " - Converted data.";
    print "   + Vector shape: ", dataset_vectors_mat.shape;
    print "   + Label shape: ", dataset_labels_mat.shape;
   
    """    ORIGINAL code
    kernels = ['linear', 'poly', 'rbf', 'sigmoid', 'precomputed'];    

    scoring_estimators = ['accuracy','average_precision','f1','f1_micro',\
        'f1_macro','f1_weighted','f1_samples','log_loss','precision',\
        'recall','roc_auc'];
    """
    # Initialize the kernel
    print "   + Label shape: ", dataset_labels_mat.shape;
    kernels = ['poly', 'rbf', 'sigmoid'];    
    scoring_estimators = ['accuracy','f1','precision','recall'];    
    
    # Earase the report file if exist
    open(sys.argv[3], 'w').close() 
    # Open report file for writing
    report = open(sys.argv[3], 'w');   
    """    ORIGINAL code
    report.write("kernel,scoring_method,ori_mean_score,ori_std,"+\
        "scale_mean_score,scale_std_score,norm_mean_score,norm_std_score,"+\
        "bin_mean_score,bin_std_score,rowsm_mean_score,rowsm_std_score," +\
        "fullsm_mean_score,fullsm_std_score," +\
        "cosine_mean_score,cosine_std_score\n");
    """   
    report.write("kernel,scoring_method,ori_mean_score,"+\
        "scale_mean_score,norm_mean_score,"+\
        "bin_mean_score,rowsm_mean_score," +\
        "fullsm_mean_score," +\
        "cosine_mean_score\n");
        
    gam = math.pow(10,(2-3*4/7));
    print " - Gamma is: ",gam;
    for ker in kernels:
        # Initialize classifier
        
        clf = svm.SVC(kernel=ker, C=10.0, gamma=gam);

        for scr in scoring_estimators:   
            try:
                # Run classifier and 3 fold cross-validation
                scores_original = cross_validation.cross_val_score(clf, \
                    dataset_vectors_mat, dataset_labels_mat,cv=5, scoring=scr);
                
                scores_scale = cross_validation.cross_val_score(clf, \
                    dataset_vectors_mat_scaled, dataset_labels_mat,cv=5, scoring=scr);
                    
                scores_norm = cross_validation.cross_val_score(clf, \
                    dataset_vectors_mat_normal, dataset_labels_mat,cv=5, scoring=scr);
                    
                scores_binariz = cross_validation.cross_val_score(clf, \
                    dataset_vectors_mat_binarization, dataset_labels_mat,cv=5, scoring=scr);
                    
                scores_rowsoft = cross_validation.cross_val_score(clf, \
                    dataset_vectors_mat_rowsoft, dataset_labels_mat,cv=5, scoring=scr);
                    
                scores_fullsoft = cross_validation.cross_val_score(clf, \
                    dataset_vectors_mat_fullsoft, dataset_labels_mat,cv=5, scoring=scr);
                
                scores_cos = cross_validation.cross_val_score(clf, \
                    dataset_vectors_mat_cos, dataset_labels_mat,cv=5, scoring=scr);
                
                print(" -> [ORIGINAL]["+ker+"] Mean "+ scr +" score: %0.2f (+/- %0.2f)"\
                        % (scores_original.mean(), scores_original.std() * 2))
                print(" -> [ SCALED ]["+ker+"] Mean "+ scr +" score: %0.2f (+/- %0.2f)"\
                        % (scores_scale.mean(), scores_scale.std() * 2))
                print(" -> [NORMALI.]["+ker+"] Mean "+ scr +" score: %0.2f (+/- %0.2f)"\
                        % (scores_norm.mean(), scores_norm.std() * 2))
                print(" -> [BINARIZ.]["+ker+"] Mean "+ scr +" score: %0.2f (+/- %0.2f)"\
                        % (scores_binariz.mean(), scores_binariz.std() * 2)) 
                print(" -> [SOFTROW.]["+ker+"] Mean "+ scr +" score: %0.2f (+/- %0.2f)"\
                        % (scores_rowsoft.mean(), scores_rowsoft.std() * 2))
                print(" -> [SOFTFULL]["+ker+"] Mean "+ scr +" score: %0.2f (+/- %0.2f)"\
                        % (scores_fullsoft.mean(), scores_fullsoft.std() * 2))
                print(" -> [ COSINE ]["+ker+"] Mean "+ scr +" score: %0.2f (+/- %0.2f)"\
                        % (scores_cos.mean(), scores_cos.std() * 2))
#    """      ORIGINAL code            
#                report.write(ker+","+scr+","+ str(scores_original.mean()) + ","\
#                            + str(scores_original.std() * 2)+ "," \
#                            + str(scores_scale.mean()) + ","\
#                            + str(scores_scale.std() * 2)+ ","\
#                            + str(scores_norm.mean()) + ","\
#                            + str(scores_norm.std() * 2)+ ","\
#                            + str(scores_binariz.mean()) + ","\
#                            + str(scores_binariz.std() * 2)+ ","\
#                            + str(scores_rowsoft.mean()) + ","\
#                            + str(scores_rowsoft.std() * 2) + ","\
#                            + str(scores_fullsoft.mean()) + ","\
#                            + str(scores_fullsoft.std() * 2) + ","\
#                            + str(scores_cos.mean()) + ","\
#                            + str(scores_cos.std() * 2) + "\n");
#    """  
                report.write(ker+","+scr+","+ str(scores_original.mean()) + ","\
#                            + str(scores_original.std() * 2)+ "," \
                            + str(scores_scale.mean()) + ","\
#                            + str(scores_scale.std() * 2)+ ","\
                            + str(scores_norm.mean()) + ","\
#                            + str(scores_norm.std() * 2)+ ","\
                            + str(scores_binariz.mean()) + ","\
#                            + str(scores_binariz.std() * 2)+ ","\
                            + str(scores_rowsoft.mean()) + ","\
#                            + str(scores_rowsoft.std() * 2) + ","\
                            + str(scores_fullsoft.mean()) + ","\
#                            + str(scores_fullsoft.std() * 2) + ","\
                            + str(scores_cos.mean()) + "\n")
#                            + str(scores_cos.std() * 2) + "\n");                          
            except:
                print "  <!> Error measuring ", scr, " for ",ker," kernel."
#                traceback.print_stack();
#                report.write(ker+","+scr+",,,,,,,,\n");
                report.write(ker+","+scr+",,,,,,\n");
    
    report.close();
            