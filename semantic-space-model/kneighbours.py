#kneighbours.py
#USAGE: python kneighbours [space file] [word] [k]
#EXAMPLE: python2.7 kneighbours.py ~/UkWac/dissect-data/ANs/out/CORE_SS.ans.ppmi.row.pkl car-n 30
#-------
from composes.utils import io_utils
from composes.similarity.cos import CosSimilarity
import sys

#load a space
my_space = io_utils.load(sys.argv[1])

#get the top 2 neighbours of "car"
print my_space.get_neighbours(sys.argv[2], int(sys.argv[3]), CosSimilarity())
