import paho.mqtt.client as mqtt_client
import random
import time
import json
import numpy as np
import connection as con #ARCHIVO QUE NOS CONECTA CON ELEPHANT
import psycopg2
from datetime import datetime, timedelta
from threading import Timer

broker = "127.0.0.1"
port = 1883
topic = "tienda/entrada1"
client_id = f'python-mqtt-{random.randint(0, 1000)}'


#funcion que genera la temperatura
def gen_temperatura():
    temperature = random.randint(35, 42)
    str_temperature = str(temperature)
    return str_temperature

def gen_cedula():
    cedula = random.randint(1000000, 35000000)
    return cedula

def tapabocas():
    tapabocas = np.random.poisson(lam=11, size=None)
    return tapabocas



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
        cursor.execute(query,cedula)
        response = cursor.fetchone()
        
        if response == None:
            return False
        else:
            return True

    except (Exception, psycopg2.Error) as error:
        print("Se ha producido el siguiente error", error)

#FUNCION DE MUESTRA PARA HACER LOS TIEMPOS DE LLEGADA DE CADA PERSONA
def hora_entrada_salida():
        hora_entrada = datetime.now()
        hora_salida = hora_entrada + timedelta(minutes = np.random.normal(50, 15))
        return [hora_entrada, hora_salida]

#funcion que nos conecta MQTT
def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)
    # Set Connecting Client ID
    client = mqtt_client.Client(client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

#publicando mensajes
def publish(client, tapaboca):
    for i in range(25):
        hora = hora_entrada_salida()
        si_tiene = True if i <= tapaboca else False
        time.sleep(5)
        msg = {
            "cedula":gen_cedula(),
            "temperatura":gen_temperatura(),
            "tapabocas": si_tiene,
            "hora_entrada": str(hora[0]),
            "hora_salida": str(hora[1]),
            }
        result = client.publish(topic, json.dumps(msg))
        print(msg)
    status = result[0]
    if status == 0:
        print(f"Send `{msg}` to topic `{topic}`")
    else:
        print(f"Failed to send message to topic {topic}")



#ahora la corrida [EL MAIN]

def run():
    client = connect_mqtt()
    while True:
        time = Timer(0.5, client.loop())
        tapaboca = tapabocas()
        publish(client, tapaboca)

if __name__ == '__main__':
    #clear()
    run()
