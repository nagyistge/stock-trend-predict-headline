# -*- coding: utf-8 -*-
"""
Created on Fri Apr 22 10:24:30 2016

@author: duy

This program aims to creat a copy of crawled data, in which raw "content" of
headline will be replaced by "cleanned" and merged text. By merged, it means 
The headlines that are similar to each other in the Distributed semantic
space will be merge text together into a single text. This text will then being
allocted back by replacing the original content of the headline to fed the
graph building step.

@input:
    - Cluster_list: list of clusters. Format: list of list of list
        + sublist contains all clusters in the same timewindow
            + subsublist contains ID of doucments in the same cluster
    - Id_list: list of headline IDs, in the same order with the iput_vector_file
    - Clean text file
@output: direct to database: the copied version of original document in crawled
    data, where contents was replaced

@usage: python 3.headline_merg.py out_clus.pkl ../data/headIds ../data/heads.tagged

"""

import sys, codecs;
import pickle;
from pymongo import MongoClient;
from bson.objectid import ObjectId;

#####################################
#	CONST DECLARATION
HOST = 'localhost';			# Host name
PORT = 27017;				# Port
DB_NAME = 'rskills';			# Name of the database
INPUT_COLLECTION_NAME	 = 'crawleddata';	# Name of input collection
OUTPUT_COLLECTION_NAME	 = 'crawleddata2';	# Name of the output collection

#####################################
#	VARIABLE DECLARATION
clean_text_dict={}; # A dictionary that map the healine ID to its clean text
                    # ObjectID -> clean text
######################################
#	BODY OF THE PROGRAM
client = MongoClient(HOST, PORT);
db = client[DB_NAME];
in_col = db[INPUT_COLLECTION_NAME];
out_col = db[OUTPUT_COLLECTION_NAME];
out_col.remove( { } ); #Clear eveything inside collog to write new thing...
print 'Connections establshed!';

# Load the clusters
clusters=pickle.load(open(sys.argv[1], "rb"));
print "Clusters loaded, size: ",len(clusters)

# Load the ID list
id_list=list(codecs.open(sys.argv[2], 'r'));
print "ID list size: ",len(id_list)
print "->1st element: ",id_list[0];

# Load the Clean text file
clean_text=list(codecs.open(sys.argv[3], 'r'));
print "Clean text size: ",len(clean_text)
print "->1st element: ",clean_text[0];

# Check lenght of files
if (len(id_list)!=len(clean_text)):
    print "Mismatch IDlist and Clean text file!";
    sys.exit();

# Build dictionay by joining both two files
for i in xrange(0,len(id_list)):
    clean_text_dict[id_list[i].strip('\n')]=clean_text[i].strip('\n');

#  Now build the collection
for time_window in clusters:
    for cls in time_window:
        #print cls;
        merged_text="";
        # concatinate all clean text of docs in the same cluster
        for ObId in cls: 
           # print ObId;
            merged_text += ". " + clean_text_dict[ObId]; 
        # write to collection
        for ObId in cls:                                                
            # get the corresponsing document from input file
            item = in_col.find_one({"_id": ObjectId(ObId)})
#            print "Find ID ", ObId, "\n -> Found type: ", type(item), " content: ",item["content"];
            # replace the content
            item["content"]=merged_text;
#            print "-> Merged content: ",item["content"];
            # insert to the output collection
            out_col.insert_one(item);
            
client.close(); # Close all connection