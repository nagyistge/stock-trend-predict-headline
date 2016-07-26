# -*- coding: utf-8 -*-
"""
Created on Wed Apr  6 12:09:11 2016

@author: duy

@arguments: 1. Pickle in counter file contain names and their freq
            2. the term to look up
"""
from __future__ import division
import sys;
from collections import Counter;
import pickle;

# Load pickle name_count
name_count = pickle.load(open(sys.argv[1], 'rb'));
print "Loaded picle " +  sys.argv[1] + " with " + str(len(name_count)) + " elements.";

# Look up the term at sys.argv[2]
term = sys.argv[2];
if (term!=""):
    ls=[]; # a list to save key and frq of related key
    total_freq = 0; # Sum total count value 
    # Now find related elements in the counting data
    for ele in name_count:
        # Check if it is a sub string
        if (term in ele):
            ls.append((ele,name_count[ele]));
            total_freq+=name_count[ele];
    # Now draw conclusion
    ls = sorted(ls, key=lambda tup: tup[1], reverse=True);
    for pair in ls:
        print '{0:20} \t {1:10} {2:10} \t {3:10}'.format(pair[0],  str(pair[1]), str(total_freq),str(pair[1]/total_freq))
else:
    print "Term can not be empty.";    