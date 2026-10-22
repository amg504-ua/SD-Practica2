import argparse
#import threading
import uuid
from Common.config import load_field_operator_config
from kafka import KafkaConsumer, KafkaProducer

estaciones = {}

#======= CONSUMIDORES =======#

def crear_consumidor_kafka(id_operador, ip, puerto_kafka, topic):
    """Crea un consumidor kafka para recibir eventos del sistema"""

    consumidor = KafkaConsumer(
        topic,
        bootstrap_servers=f"{ip}:{puerto_kafka}", #Indica donde está el broker Kafka
        group_id=f"wm-fo-{id_operador}-{topic}", #Identifica el grupo de consumidores
        auto_offset_reset="latest", #Si el grupo no tiene un offset guardado, empieza a leer los mensajes nuevos; no recupera los anteriores
        value_deserializer=lambda mensaje: mensaje.decode("utf-8"), #Convierte el mensaje, que llega como bytes, a texto
        #Este parámetro indica cómo transformar el valor de cada mensaje recibido
        
        request_timeout_ms=5000,
    )

    return consumidor

def escuchar_mensajes(consumidores):
    """Consulta los consumidores Kafka alternativamente"""

    print("Esperando mensajes de Kafka...")

    try:
        while True:
            for consumidor in consumidores:    
                mensajes = consumidor.poll(timeout_ms=500)

                for registros in mensajes.values():
                    for mensaje in registros:
                        print(f"[{mensaje.topic}] {mensaje.value}")

    except KeyboardInterrupt:
        print("\nInterrupción solicitada por el usuario.")

#======= PRODUCTORES =======#

def crear_productor_kafka(ip, puerto_kafka):
    """Crea un productor Kafka para enviar solicitudes a Central."""

    productor = KafkaProducer(
        bootstrap_servers=f"{ip}:{puerto_kafka}",
        value_serializer=lambda mensaje: mensaje.encode("utf-8"),
    )

    return productor

def enviar_solicitud_riego(productor, id_operador, id_estacion, tiempo_riego):
    """Envía una solicitud de riego a WM_Central"""

    #uuid.uuid64() genera un identificador único para distinguir esta petición de las demás
    id_peticion = str(uuid.uuid4())

    mensaje = (f"{id_peticion}#SOLICITUD_RIEGO#"
               f"{id_operador}#{id_estacion}#{tiempo_riego}")

    try:
        resultado = productor.send(
            "fo.peticion_riego.ce",
            value=mensaje,
        ).get(timeout=10)

        print(
            f"Solicitud enviada: {id_peticion} "
            f"(Topic: {resultado.topic}, "
            f"Partición: {resultado.partition})"
        )

        return id_peticion
    
    except Exception as error:
        print(f"Error al enviar la solicitud: {error}")
        return None

def solicitar_riego():
    """Recoge y valida el tiempo introducido por el operario."""

    while True:
        tiempo = input("Introduce la duración del riego en segundos: ")

        try:
            tiempo_riego = int(tiempo)

            if tiempo_riego <= 0:
                print("El tiempo de riego debe ser mayor que cero.")
                continue
            return tiempo_riego
        
        except ValueError:
            print("Entrada no válida. Introduce un número entero.")

def gestionar_solicitud_riego(productor, id_operador, id_estacion):
    """Solicita los datos del riego y envía la petición a Central."""

    tiempo_riego = solicitar_riego()

    id_peticion = enviar_solicitud_riego(productor, id_operador, id_estacion, tiempo_riego)

    return id_peticion

#def leer_fichero(fichero):

#======= MENÚ ESTACIONES =======#

def actualizar_estacion(id_estacion, estado=None, caudal=None, volumen_acumulado=None):
    """Añade una estación o actualiza los datos que recibimos de ella."""
    
    if id_estacion not in estaciones:
        estaciones[id_estacion] = {}
    datos = estaciones[id_estacion]

    if estado is not None:
        datos["estado"] = estado

    if caudal is not None:
        datos["caudal"] = caudal

    if volumen_acumulado is not None:
        datos["volumen_acumulado"] = volumen_acumulado

def mostrar_estaciones():
    """Muestra las estaciones que conocemos."""

#======= INTERFAZ =======#

#def cargar_interfaz_grafica():

#======= MAIN =======#

def main(id_operador, ip, puerto_kafka):

    config = load_field_operator_config()

    # Si no se indican argumentos, usamos los valores del .env.
    if ip is None:
        ip = config.kafka_host
    if puerto_kafka is None:
        puerto_kafka = config.kafka_port

    print(f"Operario {id_operador} iniciado")
    print(f"Broker Kafka: {ip}:{puerto_kafka}")

    consumidores = []

    try:
        consumidor_eventos = crear_consumidor_kafka(id_operador, ip, puerto_kafka, "ce.eventos.fo")
        consumidor_estados = crear_consumidor_kafka(id_operador, ip, puerto_kafka, "ce.estados.fo")

        consumidores.append(consumidor_eventos)
        consumidores.append(consumidor_estados)

        print("Consumidores Kafka creados correctamente.")

        escuchar_mensajes(consumidores)

    except Exception as error:
        print(f"Error en WM_FO: {error}")

    finally:
        for consumidor in consumidores:
            consumidor.close()
        print("Consumidores Kafka cerrados.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WM_FieldOperator")

    parser.add_argument("id_operador",help="Identificador del operario")
    #el -- hace que no sea un argumento obligatorio
    parser.add_argument("--ip",help="Dirección IP del broker kafka")
    parser.add_argument("--puerto_kafka",type=int ,help="Puerto del broker kafka")

    args = parser.parse_args()

    main(args.id_operador, args.ip, args.puerto_kafka)

#------------------------ 2 Consumidores - 2 Hilos ------------------------#
"""En caso de ser necesario, usar dos hilos para los dos consumidores"""

"""
def escuchar_mensajes(consumidor, detener):    
    Lee mensajes de un consumidor hasta que se solicite parar.
    try:
        while not detener.is_set():
            mensajes = consumidor.poll(timeout_ms=1000)

            for registros in mensajes.values():    
                for mensaje in registros:
                    print(f"[{mensaje.topic}] {mensaje.value}")

    finally:
        consumidor.close()
        print(f"Consumidor de {consumidor.subscription()} cerrado.")

"""

"""
def main(id_operador, ip, puerto_kafka):
    config = load_field_operator_config()

    if ip is None:
        ip = config.kafka_host
    if puerto_kafka is None:
        puerto_kafka = config.kafka_port

    print(f"Operario {id_operador} iniciado")
    print(f"Broker Kafka: {ip}:{puerto_kafka}")

    detener = threading.Event()
    hilos = []

    try:
        consumidor_eventos = crear_consumidor_kafka(id_operador, ip, puerto_kafka, "ce.eventos.fo")
        consumidor_estados = crear_consumidor_kafka(id_operador, ip, puerto_kafka, "ce.estados.fo")

        print("Consumidores Kafka creados correctamente.")

        for consumidor in (consumidor_eventos, consumidor_estados):
            hilo = threading.Thread(
                target=escuchar_mensajes,
                args=(consumidor, detener),
            )
            hilo.start()
            hilos.append(hilo)

        print("Escuchando eventos y estados.")

        for hilo in hilos:
            hilo.join()

    except KeyboardInterrupt:
        print("\nCerrando WM_FO...")

    except Exception as error:
        print(f"Error al conectar con kafka: {error}")
    finally:
        detener.set()

        for hilo in hilos:
            hilo.join()

        print("WM_FO finalizado.")
"""