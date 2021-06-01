#import numpy as np 
import connection as con

def usuario_preexistente(cedula):
        connection = con.get_connection()
        cursor = connection.cursor()
        query = """SELECT cedula FROM cliente WHERE cedula = (%s)"""
        cursor.execute(query, (cedula,))
        response = cursor.fetchone()
        return response

print(usuario_preexistente(8839248))