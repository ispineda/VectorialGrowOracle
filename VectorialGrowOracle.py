import cx_Oracle
import numpy as np
import os
class VectorialGrowOracle:
    
    _COSINES_ = 1
    _EUCLIDIAN_ = 2
    _DOT_PRODUCT_ = 3

    def __init__(self, host, port, sid, user, password):
        
        self.host = host
        self.port = port
        self.sid = sid
        self.user = user
        self.path_create    = 'CREATE.sql'
        self.path_delete    = 'DELETE.sql'
        self.path_package   = 'PACKAGE.sql'

        try:
            self.dsn = cx_Oracle.makedsn(self.host, self.port, self.sid)
            self.connection = cx_Oracle.connect(user = self.user, password = password, dsn= self.dsn)
            self.cursor = self.connection.cursor()
            self.sql_run_path(self.path_create)
            self.sql_run_path(self.path_package)

        except cx_Oracle.DatabaseError as e:
            print('Conexión con base de datos fallida:', e)

    def sql_run_path(self, file_path):

        path_general = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(path_general, file_path)
        
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
        host:   {self.host},
        port:   {self.port},
        sid:    {self.sid},
        user:   {self.user}
        ''')
        return (self.host,self.port,self.sid, self.user)
      
    def end_connection(self):
        self.connection.commit()
        self.cursor.close()
        self.connection.close()

    def new_collection(self, name, searchMethod=1, vectorSize=128, descripcion=None):
        if self.find_collection(name) == None:
            self.cursor.execute("INSERT INTO T_COLLECTION (NAME_COLLECTION, DESCRIPTION, SEARCH_METHOD_ID, VECTOR_SIZE) VALUES (:1,:2,:3,:4)",
            {   "1": name, 
                "2": descripcion,
                "3": searchMethod,
                "4": vectorSize})
            self.connection.commit()
            print("Se ha agregado correctamente el registro de la coleccion", name)
        else:
            print("Ya existe una coleccion con el nombre", name)

    def delete_collection(self, name):
        row = self.find_collection(name)
        
        if row != None:
            idCollection = row[0]
            self.cursor.execute("Delete from T_COLLECTION where id_collection = :id", {"id":idCollection})
            self.connection.commit()
            print("Se ha eliminado la coleccion:", name)

        else:
            print("No se ha encontrado la coleccion", name)

    def find_collection(self, name):
        self.cursor.execute("SELECT * from T_COLLECTION where NAME_COLLECTION = :name", {"name":name})
        row = self.cursor.fetchone()

        if row != None:
            print(f"""
            {name},
            {row[1]},
            {row[2]},
            {row[3]},
            {row[4]},
            {row[5]},
            """)
        return row 
    
    def add(self, nameCollection, metadata, tag, document, embeding):

        row = self.find_collection(nameCollection)        

        if row != None:
            
            sqlInsert = """
                    INSERT INTO T_DATA_VECTOR (COLLECTION_ID, METADATA,TAGS, DESCRIPTION, VECTOR)
                    VALUES (:1,:2,:3,:4,:5)            
                """
            id = row[0]
            
            tag = self.convert_type_db(tag, "V_VARCHAR2")
            embeding = self.convert_type_db(embeding, "V_BINARY_DOUBLE")

            self.cursor.execute(sqlInsert, {"1":id, 
                "2": metadata,
                "3": tag, 
                "4": document, 
                "5": embeding})
        else:
            print("Coleccion no encontrada")
        self.connection.commit()
    

    def add_documents(self, nameCollection, metadatas, tags, documents, embedings):
        
        row = self.find_collection(nameCollection)

        if row != None:
            
            sqlInsert = """
                    INSERT INTO T_DATA_VECTOR (COLLECTION_ID, METADATA,TAGS, DESCRIPTION, VECTOR)
                    VALUES (:1,:2,:3,:4,:5)            
                """

            id = row[0]
            collectionDocuments = []

            for metadata, tag, document, embeding in zip(metadatas, tags, documents, embedings):
                tag_type = self.convert_type_db(tag, "V_VARCHAR2")
                embeding_type = self.convert_type_db(embeding, "V_BINARY_DOUBLE")

                collectionDocuments.append((id, metadata, tag_type, document, embeding_type))

            
            self.cursor.executemany(sqlInsert, collectionDocuments)

        self.connection.commit()

    def convert_type_db(self, vector, type):

        vector_type = self.connection.gettype(type)
        vector_instance = vector_type.newobject()

        for element in vector:
            vector_instance.append(element)
        return vector_instance
    
    def search_vector(self, name, vector, k=5):
        row = self.findCollection(name)

        if row != None:
            vector = self.convert_type_db(vector, 'V_BINARY_DOUBLE')
            ref_cursor = self.cursor.var(cx_Oracle.CURSOR)

            self.cursor.callproc("DBVECTORIAL.SEARCH_VECTOR",[name, vector, k, ref_cursor])

            print(ref_cursor[0])
        else:
            print("Coleccion no encontrada")

