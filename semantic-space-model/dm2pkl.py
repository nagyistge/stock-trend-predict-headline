#Convert .dm file to .pkl
#Usage: python dm2pkl bnc.dm

from composes.semantic_space.space import Space
from composes.utils import io_utils
import sys

space = Space.build(data=sys.argv[1], format='dm')
name = sys.argv[1][0:-3]
io_utils.save(space, name+".pkl")
