import numpy as np
from VectorialGrowOracle import *

if __name__ == "__main__":
    bdVectorial = VectorialGrowOracle( host='localhost',
                                    port="1521",
                                    sid='CURSO',
                                    user="HR",
                                    password='HR')

    bdVectorial.create_collection('Armando',137,'cosines')
    ##bdVectorial.delete_collection('Armando')
    ##bdVectorial.end_connection()
    ##bdVectorial.delete_all()
    # Configura la conexión a la base de datos
    