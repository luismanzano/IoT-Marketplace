import paho.mqtt.client
import psycopg2
from psycopg2 import Error
import json
import connection as con #ARCHIVO QUE NOS CONECTA CON ELEPHANT
import datetime
import time

#obtener el ultimo minuto para insertar el siguiente
def usuario_preexistente(cedula):
    try:
        connection = con.get_connection()
        cursor = connection.cursor()
        query = """SELECT cedula FROM cliente WHERE cedula = (%s)"""
        cursor.execute(query, (cedula,))
        response = cursor.fetchone()
        
        if response == None:
            return False
        else:
            return True

    except (Exception, psycopg2.Error) as error:
        print("Se ha producido el siguiente error", error)



def usuario_afiliado(cedula):
    try:
        connection = con.get_connection()
        cursor = connection.cursor()
        query = """SELECT cedula FROM afiliado WHERE cedula = (%s)"""
        cursor.execute(query, (cedula,))
        response = cursor.fetchone()
        
        if response == None:
            return False
        else:
            return True

    except (Exception, psycopg2.Error) as error:
        print("Se ha producido el siguiente error", error)



def on_message(client, userdata, message):
	#Traduccion de la data porque se recibe codificado
    aux = json.loads(message.payload.decode('utf-8'))
	#print(json.loads(message.payload.decode('utf-8')))
    try:
        if(aux['tapabocas']==False):
            print('No tengo tapa')
        else:
            if(int(aux['temperatura'])>38):
                print('Temperatura muy alta')
            elif (int(aux['temperatura'])>=40):
                print('AMBULANCIA')
            else:
                if (usuario_preexistente(int(aux['cedula']))==False):
                    connection = con.get_connection()
                    cursor = connection.cursor()
                    insert_query = """INSERT INTO cliente (last_update, cedula) VALUES (%s,%s)"""
                    item_tuple = (aux['hora_entrada'],aux['cedula'])
                    cursor.execute(insert_query, item_tuple)
                    connection.commit()
                    print('Insercion usuario no registrado ', item_tuple)
                    cursor.close()
                    connection.close()
                else:
                    print('usuario registrado')
                connection = con.get_connection()
                cursor = connection.cursor()
                insert_query = """INSERT INTO entrada_cliente (cliente_cedula,
                    sucursal_id, temperatura,tapabocas,hora_entrada) VALUES (%s,%s,%s,%s,%s)"""
                sucursal = 1
                item_tuple = (aux['cedula'], sucursal ,aux['temperatura'],
                aux['tapabocas'],aux['hora_entrada'])
                cursor.execute(insert_query, item_tuple)
                connection.commit()
                print('Insercion entrada_cliente',item_tuple)
                cursor.close()
                connection.close()
                    
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)


	#try:
		#cursor = connection.cursor()
		# Query
		#insert_query = """ INSERT INTO tanque (nivel, report_time) VALUES (%s, %s)"""
	
		#item_tuple = (aux['nivel_Tanque'], hora)
		#cursor.execute(insert_query, item_tuple)
		#connection.commit()

	#except (Exception, psycopg2.Error) as error:
	#	print("Error while connecting to PostgreSQL", error)

	
def on_connect(client,userdata,flags,rc):
	print('Conectado entrada 1')
	client.subscribe(topic='tienda/entrada1', qos=2)

def susEntrada1():
	client = paho.mqtt.client.Client(client_id='entrada1', clean_session=False)
	client.connect(host='localhost', port=1883)
	client.on_connect = on_connect
	client.on_message = on_message
	client.loop_forever()

def main():
	susEntrada1()


if __name__ == '__main__':
	main()

sys.exit(0)
	


