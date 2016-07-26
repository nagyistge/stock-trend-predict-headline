#similarity.py
#USAGE: python similarity [space file] [word1] [word2]
#EXAMPLE: python kneighbours ~/UkWac/dissect/ANs/ANs.kpl car_n dog_n
#-------
from composes.utils import io_utils
from composes.similarity.cos import CosSimilarity
import sys

#load a space
my_space = io_utils.load(sys.argv[1])

#print my_space.cooccurrence_matrix
#print my_space.id2row

#compute similarity between two words in the space
print "The similarity of",sys.argv[2],"and",sys.argv[3],"is:",my_space.get_sim(sys.argv[2], sys.argv[3], CosSimilarity())
