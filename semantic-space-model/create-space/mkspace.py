from composes.semantic_space.space import Space
from composes.utils import io_utils
from composes.transformation.scaling.ppmi_weighting import PpmiWeighting
from composes.transformation.scaling.row_normalization import RowNormalization
from composes.transformation.dim_reduction.svd import Svd;

import sys

#create a space from co-occurrence counts in sparse format
my_space = Space.build(data = "../data/"+sys.argv[1]+".sm",
                       rows = "../data/"+sys.argv[1]+".rows",
                       cols = "../data/"+sys.argv[1]+".cols",
                       format = "sm")
                       
my_space = my_space.apply(PpmiWeighting())
my_space = my_space.apply(RowNormalization())

#apply svd reduction
my_space = my_space.apply(Svd(1500))

    
#export the space in dense format and pkl format
my_space.export("../spaces/"+sys.argv[1], format = "dm")
io_utils.save(my_space, "../spaces/"+sys.argv[1]+".pkl")
