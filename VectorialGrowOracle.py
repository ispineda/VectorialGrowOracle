import cx_Oracle
import os
import numpy as np

class VectorialGrowOracle:
    
    def __init__(self, host, port, sid, user, password):
        
        self.host = host
        self.port = port
        self.sid = sid
        self.user = user
        self.connection_available = False
        self.path_create    = 'CREATE.sql'
        self.path_delete    = 'DELETE.sql'
        self.path_package   = 'PACKAGE.sql'

        try:
            self.dsn = cx_Oracle.makedsn(self.host, self.port, self.sid)
            self.connection = cx_Oracle.connect(user = self.user, password = password, dsn= self.dsn)
            self.cursor = self.connection.cursor()
            self.sql_run_path(self.path_create)
            self.sql_run_path(self.path_package)
            self.connection_available = True

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
        conexión disponible: {self.connection_available}
        ''')
        return (self.host,self.port,self.sid, self.user)
      
    def end_connection(self):
        self.connection.commit()
        self.cursor.close()
        self.connection.close()

