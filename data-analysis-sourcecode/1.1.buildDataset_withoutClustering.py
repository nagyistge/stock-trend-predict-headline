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
   python 1.buildDataset.py sem_space n_threads dataset_vector_f ataset_label_f
E.g from command line: 
"python 1.buildDataset.py ../spaces/heads.pkl 4 dataset.vectors dataset.labels"
    
Parameters:
    - sem_space: the semantic space
    - n_threas: number of threads to run
    - [OUT] dataset_vector_f: name of the dataset vector file
    - [OUT] dataset_label_f: name of the dataset vector file
    
Description: this is the first python script for the project
    The program build a dataset from headlines and stock data.
    How it works? For a give time window, it...
        - query all headlines from DB, extract feature using a pre-built
        Distributional Semantic. This play the role of vector for train/test
        - query stock value from database, then consider the increase/decrease
        of stock based on the value of Open at the the biginning of time window
        and the value of Close at the end of time window.
"""
from utils import *;
#from functions import *;
from datetime import datetime,timedelta;
import sys,pickle,pymongo;
from pymongo import MongoClient;
from scipy.sparse import csr_matrix,vstack;

"""
import sys, codecs,traceback,re, threading;
from composes.utils import io_utils;
from composes.semantic_space.space import Space;
from  composes.matrix.sparse_matrix import SparseMatrix;
import numpy as np
from scipy import sparse
import pickle
"""
###############################################################################
#	CONST DECLARATION
HOST = 'localhost';			# Host name
PORT = 27017;				# Port
DB_NAME = 'rskills';			# Name of the database
COLLECTION_NAME	 = 'crawleddata';	# Name of the collection
STOCK_COLLECTION_NAME='stockdata_USA300m' # Name of the stock data collection
LOG_COLLECTION_NAME	 = 'commandlog';	# Name of the log collection
RESULT_LOG_COLLECTION_NAME	 = 'resultlog2';# Name of the result log collection
# The time to start query from (the CRAWLED TIME)
QUERY_START_TIME = datetime(2015, 10, 14, 16, 0, 0, 0);	
# Deltatime, so query will get data from QUERY_START_TIME yo QUERY_START_TIME +
# DELTA_TIME
DELTA_TIME = timedelta(minutes=60);
TIME_ZONE_SHIFT = timedelta(minutes=420);	  # Since data was collect in VN (GMT+7)
# At the beginning corpust, freq of edge and node always be low, so we put 
# a bias to disregard prunning at the beginning
STARTING_PRUNNING_PREVENTION_BIAS = 0;  
# Maximum number of corpus to be process. 
# This is the Hard-breaking point of the program
MAXIMUM_NUMBER_OF_CORPUS = 999999999999999;

################################# MAIN PROGRAM ################################
print(__doc__);

######################################
#	BODY OF THE PROGRAM
if __name__ == '__main__':
    client = MongoClient(HOST, PORT);
    db = client[DB_NAME];
    col = db[COLLECTION_NAME];
    sto_col = db[STOCK_COLLECTION_NAME];
    print 'Connections establshed!';
    current_time = QUERY_START_TIME;	# time point to start querying
    count = 0;          # Count number of corpus queried
    result_count=0;     # Count number of corpus whose enconted stock changes
    time_out_count = 0; # This count number of continuous time the program 
                        #   can't get stock/text data from DB
    
    my_space = None; # The semantic space
    space_dim = 1500; # Number of dims of the semantic space
    key_set = set([]); # store set of keys of the semantic space
    
    # Combine RE pattern
    pattern = re.compile('\W')
    Load_Semantic_Space(sys.argv[1]);
    
    dataset_vectors_mat=None;
    dataset_labels_list=[];
    
    previous_data_phrase_succeeded=True;  
    
    while (current_time < datetime.now() and time_out_count < 50):
        start_time = time.time();
        block = [];		# Block of all collected records in the time window
        count +=1;
        
        print '\n####################################\nEntered time window '\
                + str(count) + ':-) ', current_time, ' to ', current_time + \
                DELTA_TIME;
                
        ######################### STOCK DATA
        # get the stock value
        print ' - Start query from Stock DB...';
        records = sto_col.find({"Converted_GMT_time": {"$gte": current_time - \
                    TIME_ZONE_SHIFT, "$lte": current_time - TIME_ZONE_SHIFT - \
                    timedelta(minutes=1) + DELTA_TIME}});
        if (records.count()==0): # Check number of result
             print " --> No stock data received!. Go to next time window.";                
             current_time = current_time + DELTA_TIME;
             if (previous_data_phrase_succeeded):
                 time_out_count = 1;
             else:
                 time_out_count += 1;
             previous_data_phrase_succeeded = False;
             print "    <!> Current time_out_count: ",time_out_count;
             continue;
            
        # Get earliest OPEN value
        earliest_record= records.sort("Converted_GMT_time", pymongo.ASCENDING)\
                            .limit(1);
        open_value = earliest_record[0]['Open'];
        print " -> Earliest open  value: ",open_value,"@",\
                earliest_record[0]['Converted_GMT_time']+TIME_ZONE_SHIFT;
        
        # Get latest CLOSE value        
        latest_record = records.sort("Converted_GMT_time", pymongo.DESCENDING)\
                            .limit(1);
        close_value = latest_record[0]['Close'];
        print " --> Latest close value: ",close_value,"@",\
                latest_record[0]['Converted_GMT_time']+TIME_ZONE_SHIFT;
        
        # Copute the change:
        stock_change =  close_value - open_value;
        label = None;           # Value assiged to the sample
        if (stock_change>0):    # close value larger than open value
            label = +1;         # +1 -> positive
        elif (stock_change<0):  # close value smaller than opem value
            label = -1;         # -1 -> negative
        else:
            label = 0;          # No stock change, maybe the market closed
            print "      --> Zero stock change detected. Moving to the next.";
            current_time = current_time + DELTA_TIME;
            print "    <!> Current time_out_count: ",time_out_count;
            continue;           # Move to next time window
            
        print "      --> Assigned value: ", label;
        
        ######################### HEADLINES DATA 
        print ' - Start query from headline DB...';
    	#sys.stdout.write(' - Start query from DB...' + '\n');
    	
        print " --> Query statement: col.find({\"time\": {\"$gt\": "+ \
                str(current_time) +", \"$lt\": "+ \
                str(current_time + DELTA_TIME)+"}}";
        records = col.find({"time": {"$gt": current_time, \
                                         "$lt": current_time + DELTA_TIME}});
        if (records.count()==0): # Check number of result
             print "      --> No text data received!. Go to next time window.";                
             current_time = current_time + DELTA_TIME;
             if (previous_data_phrase_succeeded):
                 time_out_count = 1;
             else:
                 time_out_count += 1;
             previous_data_phrase_succeeded = False;
             print "    <!> Current time_out_count: ",time_out_count;
             continue;
             
        for record in records:
            block.append(record);
        print " --> Data block size: ", len(block);
        # Now pre-process the text
        texts = TextPreprocess(block);
        print " - After preprocess, data size is: ", len(texts);
        print " - Start feature extraction...", len(texts);
        
        # Get number of threads
        nthreads = int(sys.argv[2]);  
        
        # Extract feature
        dsspace = TextExtraction(texts,nthreads);
        
        # Run DBScan
        #clusters = RunDBSCAN(dsspace.cooccurrence_matrix,dsspace.id2row);
        # print clusters;
        #cluster_vector = ClusterVectorExtraction(dsspace,clusters);
        cluster_vector = csr_matrix(dsspace.cooccurrence_matrix.mat.sum(axis=0));
        #print "------------------->",cluster_vector;
       
        # Record data        
        if (count==1):
            dataset_vectors_mat = cluster_vector;
        else:
            dataset_vectors_mat = vstack([dataset_vectors_mat,cluster_vector]); 
        dataset_labels_list.append(label);
        result_count+=1;
        
        # BREAK IT WHEN DONE
        if (count>10000):
            break;
        
        # Move to the next time window
        current_time = current_time + DELTA_TIME;
        time_out_count=0;
        previous_data_phrase_succeeded = True;

    # Finalize the data
    dataset_labels = csr_matrix(dataset_labels_list);
    
    print " - Finalizing the dataset building: ";
    print " --> Dataset vector matrix size: ",dataset_vectors_mat.get_shape();
    dataset_vector_f = open(sys.argv[3], 'w')    
    pickle.dump(dataset_vectors_mat, dataset_vector_f);
    dataset_vector_f.close();
    print " --> Dataset label matrix size: ",dataset_labels.get_shape();
    dataset_label_f = open(sys.argv[4], 'w')    
    pickle.dump(dataset_labels, dataset_label_f);
    dataset_label_f.close();