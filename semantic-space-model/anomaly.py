#-------
# This script computes several semantic anomaly measures
# It takes as input: 1) a semantic space in pkl format
# 2) a file with short phrases (2 words, e.g. parliamentary potato)
#-------
from composes.utils import io_utils
from composes.utils import scoring_utils
from composes.similarity.cos import CosSimilarity
from composes.composition.weighted_additive import WeightedAdditive
from composes.composition.multiplicative import Multiplicative
from composes.transformation.scaling.row_normalization import RowNormalization
import numpy as np
import sys

#read in a space
my_space = io_utils.load(sys.argv[1])
my_space = my_space.apply(RowNormalization())

add = WeightedAdditive(alpha = 1, beta = 1)
mult = Multiplicative()


#compute multiplication/addition of a list of word pairs
fname = sys.argv[2]
word_pairs = io_utils.read_tuple_list(fname, fields=[0,1])

lengths=[]
found=True
for wp in word_pairs:
	try:
		v1=my_space.get_row(wp[0])
		v2=my_space.get_row(wp[1])
	except KeyError:
		#print wp[0],"or",wp[1],"not found"
		found=False
	if found:
		composed_space = add.compose([(wp[0], wp[1], "_composed_")], my_space)
		neighbours=composed_space.get_neighbours("_composed_", 10, CosSimilarity(),space2=my_space)
		print wp[0],wp[1]
		print neighbours
		density=0
		for n in neighbours:
			density+=n[1]
		density=density/10
		print "Density",density
		c=composed_space.get_row("_composed_")
		print "Norm ",c.norm()
		cos=composed_space.get_sim("_composed_",wp[1], CosSimilarity(), space2=my_space)
		print "Cos ",cos
		print "--"
	else:
		found=True

