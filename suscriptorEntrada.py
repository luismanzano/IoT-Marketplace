import paho.mqtt.client
import psycopg2
from psycopg2 import Error
import json
import connection as con #ARCHIVO QUE NOS CONECTA CON ELEPHANT
import datetime
import time
import random

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


def buscar_productos():
    try:
        connection = con.get_connection()
        cursor = connection.cursor()
        query = """SELECT p.producto_id, p.nombre, ep.estante_id FROM estante e
                    INNER JOIN estante_producto ep ON e.estante_id = ep.estante_id
                    INNER JOIN producto p ON ep.producto_id = p.producto_id
                    WHERE e.sucursal_id = 1"""
        cursor.execute(query)
        response = cursor.fetchall()
        cursor.close()
        connection.close()

        a = random.randint(0,len(response))
        b = random.randint(0,len(response))
        c = random.randint(0,len(response))
    
        array = response[a][0],response[a][2], response[b][0],response[b][2],response[c][0],response[c][2]

        return array

    except (Exception, psycopg2.Error) as error:
        print("Se ha producido el siguiente error", error)

def buscar_carrito(cedula):
    try:
        connection = con.get_connection()
        cursor = connection.cursor()
        query = """SELECT ca.carrito_id FROM carrito ca
                    INNER JOIN cliente cl ON ca.cliente_id = cl.cliente_id
                    WHERE cl.cedula = (%s)"""
        cursor.execute(query,(cedula,))
        response = cursor.fetchone()
        cursor.close()
        connection.close()

        return response

    except (Exception, psycopg2.Error) as error:
        print("Se ha producido el siguiente error", error)



def usuario_afiliado(cedula):
    try:
        connection = con.get_connection()
        cursor = connection.cursor()
        query = """SELECT cedula FROM afiliado WHERE cedula = (%s)"""
        cursor.execute(query, (cedula,))
        response = cursor.fetchone()
        cursor.close()
        connection.close()

        if response == None:
            return False
        else:
            return True

    except (Exception, psycopg2.Error) as error:
        print("Se ha producido el siguiente error", error)



def cola_salida(duracion):
    if len(contador)<20:
        contador.append(duracion)
    else:
        cola.append(duracion)

    for i in range(len(contador)):
        if i != 0:
            contador[i] -= contador[0]
            
            if contador[i] < 0:
                contador[i] = 0

    contador.sort()
    
    if contador[0] < 0:
        time.sleep(contador[0])
    contador.pop(0)

    

    if len(contador) < 20 and len(cola) > 0:
        libre = 20 - len(contador)
        for i in range(libre):
            contador.append(cola[0])
            cola.pop(0)


contador = []
cola = []


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

                query_productos = buscar_productos()
                productos = [query_productos[0],query_productos[2],query_productos[4]]
                estantes = [query_productos[1],query_productos[3],query_productos[5]]
                carrito = buscar_carrito(aux['cedula'])

                connection = con.get_connection()
                cursor = connection.cursor()
                for i in range(len(productos)):
                    insert_query = """INSERT INTO carrito_producto (carrito_id,
                        producto_id, cantidad,costo,estante_id) VALUES (%s,%s,%s,%s,%s)"""
                    item_tuple = (carrito, productos[i] ,random.randint(1,3),
                    0,estantes[i])
                    cursor.execute(insert_query, item_tuple)
                    connection.commit()
                    print('Insercion carrito_producto',item_tuple)
                cursor.close()
                connection.close()



                connection = con.get_connection()
                cursor = connection.cursor()
                insert_query = """INSERT INTO factura (cedula_cliente,
                    sucursal_id, costo,fecha,met_pago) VALUES (%s,%s,%s,%s,%s)"""
                item_tuple = (aux['cedula'], 1 ,0,
                    aux['hora_salida'],random.randint(1,3))
                cursor.execute(insert_query, item_tuple)
                connection.commit()
                print('Insercion factura',item_tuple)
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
	


