##########################################################################
# Run analogies of the type queen-woman+man=king
# USAGE: python ./analogy.py ../spaces/wikipedia.pkl Paris France Germany
# (Paris is to France what ___ is to Germany)
##########################################################################

from composes.utils import io_utils
from composes.composition.weighted_additive import WeightedAdditive
from composes.similarity.cos import CosSimilarity
import sys



pkl=sys.argv[1]
base=sys.argv[2]
minus=sys.argv[3]
plus=sys.argv[4]

space = io_utils.load(pkl)

# instantiate an additive and subtractive model
add = WeightedAdditive(alpha = 1, beta = 1)
sub = WeightedAdditive(alpha = 1, beta = -1)


#print space.get_neighbours(base, 10, CosSimilarity())

print "Subtracting",minus,"from",base
composed_space = sub.compose([(base, minus, "step1")], space)
#print composed_space.get_neighbours("step1", 10, CosSimilarity(),space)

print "Adding",plus,"..."
composed_space2 = add.compose([("step1", plus, "step2")], (composed_space,space))
print composed_space2.get_neighbours("step2", 10, CosSimilarity(),space)
