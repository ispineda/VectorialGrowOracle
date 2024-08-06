import cx_Oracle
import numpy as np
import os

class VectorialGrowOracle:
    
    def __init__(self, host, port, sid, user, password):
        
        self.host = host
        self.port = port
        self.sid = sid
        self.user = user
        self.connection_available = False
        self.path_create = 'CREAR.sql'
        self.path_delete = 'ELIMINAR.sql'

        try:
            self.dsn = cx_Oracle.makedsn(self.host, self.port, self.sid)
            self.connection = cx_Oracle.connect(user = self.user, password = password, dsn= self.dsn)
            self.cursor = self.connection.cursor()
            self.sql_run_path(self.path_create)
            self.connection_available = True

        except cx_Oracle.DatabaseError as e:
            print('Conexión con base de datos fallida:', e)

    def sql_run_path(self, file_path):

        path_general = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(path_general, file_path)
        print(file_path)
        
        with open(file_path, 'r') as file:
            sql_script = file.read()
        
        sql_commands = sql_script.split('/--split')
        for i, command in enumerate(sql_commands):
            command = command.strip()            
            self.cursor.execute(command)
        
        self.connection.commit()
    
    def delete_all(self):
        self.sql_run_path(self.path_delete)

    def get_data_connection(self):
        print(f'''
        host: {self.host},
        port: {self.port},
        sid: {self.sid},
        user: {self.user}
        conexión disponible: {self.connection_available}
        ''')
        return (self.host,self.port,self.sid, self.user)
    
    def get_collection_details(self, name):
        
        self.name_collection = name

        len_vector = self.cursor.var(cx_Oracle.NUMBER)
        search_method = self.cursor.var(cx_Oracle.STRING)
        creation_date = self.cursor.var(cx_Oracle.DB_TYPE_DATE)
        
        # Llamar al procedimiento
        self.cursor.callproc('DBVECTORIAL.GET_COLLECTION_DETAILS', [
            self.name_collection,
            len_vector,
            search_method,
            creation_date
        ])

        if len_vector.getvalue() == None and search_method.getvalue() == None and creation_date.getvalue() == None:
            return None

        print(f'''
        name_collection: {self.name_collection},
        len_vector: {len_vector.getvalue()},
        search_method: {search_method.getvalue()},
        creation_date: {creation_date.getvalue()}
        ''')

        return {
                'name_collection': self.name_collection,
                'len_vector': len_vector.getvalue(),
                'search_method': search_method.getvalue(),
                'creation_date': creation_date.getvalue()
            }
    
    def delete_collection(self, name_collection):
        self.cursor.callproc('DBVECTORIAL.DELETE_COLLECTION', [name_collection])
        print('Colección eliminada')

    def create_collection(self, name_collection, len_vector, method):
    
        if self.get_collection_details(name_collection) is None:
            try:
                
                # Llamar al procedimiento
                self.cursor.callproc('DBVECTORIAL.CREATE_COLLECTION', [
                    name_collection,
                    len_vector,
                    method
                ])

                print('Colección insertada exitosamente')

            except cx_Oracle.DatabaseError as e:
                error, = e.args
                print(f'Error de base de datos: {error.message}')
            
            except Exception as e:
                print(f'Error inesperado: {e}')
        else:
            print('Ya existe la colección')
        
        self.get_collection_details(name_collection)
            
    def end_connection(self):
        self.connection.commit()
        self.cursor.close()
        self.connection.close()

if __name__ == "__main__":
    bdVectorial = VectorialGrowOracle( host='localhost',
                                    port="1521",
                                    sid='CURSO',
                                    user="HR",
                                    password='HR')

    ##bdVectorial.create_collection('Armando',137,'cosines')
    ##bdVectorial.delete_collection('Armando')
    ##bdVectorial.end_connection()
    ##bdVectorial.delete_all()
    # Configura la conexión a la base de datos
    