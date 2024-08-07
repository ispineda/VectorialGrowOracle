import numpy as np
from VectorialGrowOracle import *

if __name__ == "__main__":
    bdVectorial = VectorialGrowOracle(  host='localhost',
                                        port="1521",
                                        sid='CURSO',
                                        user="HR",
                                        password='HR')

    len_model = 137
    name_collection_v = 'ollama_documents_do'
    bdVectorial.create_collection(name_collection_v,len_model,'cosines')

    
    ## Simula datos de embeding
    vector = np.random.random(len_model).astype(np.float64)
    ##vector  = np.arange(len_model).astype(np.float64)
    tags    = np.array(['DOCUMENTO1','DOCUMENTO2','DOCUMENTO3'])
    description = 'Cualquier descripcion cuenta'

    bdVectorial.add_vector(
                        name_collection_v,
                        tags,
                        description,
                        vector)

    ##bdVectorial.delete_collection('Armando')
    ##bdVectorial.delete_all()
    bdVectorial.end_connection()
    
    