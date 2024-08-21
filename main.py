import numpy as np
from VectorialGrowOracle import *
from db_conexion import *

if __name__ == "__main__":
    bdVectorial = VectorialGrowOracle(  host=SET_HOST,
                                        port=SET_PORT,
                                        sid=SET_SID,
                                        user=SET_USER,
                                        password=SET_PASSWORD)

    len_model = 128
    name_collection_v = 'OLLAMA'

    
    # ## Simula datos de embeding
    # bdVectorial.new_collection(name_collection_v, searchMethod = bdVectorial._EUCLIDIAN_)
    # emebdings = [np.random.random(len_model).astype(np.float64) for i in range(2)]
    # metadatas    = ['{"1":"1"}' for i in range(2)]
    # tags = [["Etiqueta1","Etiqueta2"] for i in range(2)]
    # descriptions = [f'{i}' for i in range(2)]

    # bdVectorial.add_documents(name_collection_v, metadatas, tags, descriptions, emebdings)

    embeding = np.random.random(len_model).astype(np.float64)

    bdVectorial.search_vector(name_collection_v,embeding, 2)
    