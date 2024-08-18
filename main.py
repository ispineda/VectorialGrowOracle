import numpy as np
from VectorialGrowOracle import *
from db_conexion import *

if __name__ == "__main__":
    bdVectorial = VectorialGrowOracle(  host=SET_HOST,
                                        port=SET_PORT,
                                        sid=SET_SID,
                                        user=SET_USER,
                                        password=SET_PASSWORD)

    len_model = 137
    name_collection_v = 'collection1'

    
    ## Simula datos de embeding
    vector = np.random.random(len_model).astype(np.float64)
    tags    = np.array(['DOCUMENTO1','DOCUMENTO2','DOCUMENTO3'])
    description = 'Cualquier descripcion cuenta'

    ##bdVectorial.delete_all()
    ##bdVectorial.end_connection()
    
    