import socket
import threading
import BD

#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from kafka import KafkaProducer, KafkaConsumer
from Common.config import load_central_config

"""COMUNICACIÓN MEDIANTE SOCKETS"""

HEADER = 64
FORMAT = 'utf-8'

def recibir_exacto(conn, cantidad):
    """Recibe exactamente la cantidad de bytes solicitada."""
    datos = bytearray()

    while len(datos) < cantidad:
        fragmento = conn.recv(cantidad - len(datos))

        if not fragmento:
            return None

        datos.extend(fragmento)

    return bytes(datos)

def handle_client(conn, addr):
    print(f"[NUEVA CONEXION] {addr} connected.")
    #variable para guardar la estacion en la bd
    id_estacionA = None 
    
    connected = True
    while connected:#infinito, cambiar para la practica
        try: 
            msg_length = conn.recv(2048).decode(FORMAT)
            if msg_length:
                for msg in msg_length.strip().split("\n"):
                    msg = msg.strip()
                    if not msg:
                        continue
                    print(f" He recibido del cliente [{addr}] el mensaje: {msg}")

                    partes = msg.split("#")
                    tipo = partes[0]
                    #si la estacion se registra
                    if tipo == "REGISTRO" and len(partes) == 3:
                        id_estacion = partes[1]
                        ubicacion = partes[2]
                        id_estacionA = id_estacion
                        
                        print(f"Estacion: {id_estacion} en {ubicacion}")
                        #guardar en la bd
                        BD.guardar_estacion(id_estacion, ubicacion)

                        respuesta = f"REGISTRO_OK#{id_estacion}"
                    
                    #si la estacion se quiere conectar y ya esta registrada
                    elif tipo == "CONEXION" and len(partes) == 2:
                        id_estacion = partes[1]
                        id_estacionA = id_estacion
                        BD.modificar_estados(id_estacion, "DISPONIBLE")
                        print(f"Estacion disponible: {id_estacion}")
                        respuesta = f"CONEXION_OK#{id_estacion}"

                    #si la estacion se quiere conectar y ya esta registrada
                    elif tipo == "AVISO_FUGA" and len(partes) == 2:
                        id_estacion = partes[1]
                        BD.modificar_estados(id_estacion, "DESCONECTADA")
                        print(f"Estacion en fuga: {id_estacion}")
                        respuesta = f"AVISO_FUGA_RECIBIDO#{id_estacion}"
                    
                    #si la estacion se quiere conectar y ya esta registrada
                    elif tipo == "FUGA_RESUELTA" and len(partes) == 2:
                        id_estacion = partes[1]
                        BD.modificar_estados(id_estacion, "DISPONIBLE")
                        print(f"Estacion CON FUGA RESUELTA: {id_estacion}")
                        respuesta = f"FUGA_RESUELTA_CONFIRMACION#{id_estacion}"
                    
                    elif tipo in ["ORDEN_OoO_RECIBIDO", "ORDEN_ACTIVACION_RECIBIDA"]: 
                        print(f"Estacion disponible: {id_estacion}")
                        respuesta = f"CONEXION_OK#{id_estacion}"

                    else:
                        respuesta = F"ERROR#Formato incorrecto {msg}"

                    conn.send((respuesta + "\n").encode(FORMAT))
            else:
                print(f"DESCONEXION DEL MONITOR EN {addr}")
                connected = False

        except (ConnectionError, OSError) as exc:
            print(f"[ERROR DE CONEXIÓN] {addr}: {exc}", flush=True)

    if id_estacionA is not None:
        BD.modificar_estados(id_estacionA, "DESCONECTADA")
        print(f"Estacion {id_estacionA} pasa a desconectada")
    
    conn.close()
    print(f"Conecion cerrada {addr}", flush=True)

def servidor_sockets(puerto):
    SERVER = socket.gethostbyname(socket.gethostname())
    ADDR = (SERVER, puerto)

    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        servidor.bind(("0.0.0.0", puerto))
        servidor.listen()

        print(
            f"[CENTRAL] Servidor escuchando en 0.0.0.0:{puerto}",
            flush=True,
        )

        while True:
            conn, addr = servidor.accept()

            hilo = threading.Thread(
                target=handle_client,
                args=(conn, addr),
                daemon=True,
            )
            hilo.start()

    finally:
        servidor.close()

"""COMUNICACIÓN MEDIANTE KAFKA"""

#======= PRODUCTORES =======#

def crear_productor_kafka(ip,puerto):
    productor = KafkaProducer(
        bootstrap_servers=f"{ip}:{puerto}",
        value_serializer=lambda mensaje: mensaje.encode("utf-8")
    )

    return productor

def enviar_estado(productor, id_estacion, estado, ubicacion, caudal="-", volumen="-", id_operario="-"):
    
    mensaje = (f"{id_estacion}#{estado}#{ubicacion}#{caudal}#{volumen}#{id_operario}")

    try:
        resultado = productor.send("ce.estados.fo", value=mensaje).get(timeout=10)
        print(f"[KAFKA] Estado enviado: {mensaje} (Topic: {resultado.topic})", flush=True)
    
    except Exception as error:
        print(f"[KAFKA] Error al enviar el estado: {error}", flush=True)

#======= CONSUMIDORES =======#

def crear_consumidor_kafka(ip, puerto, topic):
    consumidor = KafkaConsumer(
        topic,
        bootstrap_servers=f"{ip}:{puerto}",
        group_id="wm-central-peticiones-riego",
        auto_offset_reset="latest",
        value_deserializer=lambda mensaje: mensaje.decode("utf-8")
    )

    return consumidor

def escuchar_solicitudes_riego(consumidor, detener):
    try:
        print("[KAFKA] Escuchando solicitudes de riego...")

        while not detener.is_set():
            mensajes = consumidor.poll(timeout_ms=500)

            for registros in mensajes.values():
                for mensaje in registros:
                    print(
                        f"[SOLICITUD RECIBIDA] {mensaje.value}"
                    ) 
    finally:
        consumidor.close()

"""MAIN"""

def central():
    '''Esto seguramente lo divida en dos funciones diferentes, pero
    es para que se entienda el concepto.'''
    config = load_central_config()

    print("[CENTRAL] Iniciando...")
    print(
        f"[CENTRAL] Kafka: "
        f"{config.kafka_host}:{config.kafka_port}"
    )

    detener = threading.Event()
    consumidor = None
    productor = None
    hilo_kafka = None
    hilo_sockets = None
    
    """BASE DE DATOS"""

    BD.inicializar()
    estaciones = BD.devolver_estaciones()
    for estacion in estaciones:
        print(f"[ESTACIÓN] : {estacion[0]}, [UBICACIÓN] : {estacion[1]}, [ESTADO] : {estacion[2]} .")

    try:
    
        """KAFKA"""
        consumidor = crear_consumidor_kafka(config.kafka_host, config.kafka_port, "fo.peticion_riego.ce")
        productor = crear_productor_kafka(config.kafka_host, config.kafka_port)

        enviar_estado(productor, "WS_TEST", "DISPONIBLE", "Pruebas")
                
        print("[CENTRAL] Conexiones Kafka creadas.")

        #ESTACIÓN CREADA PRUEBAS
        BD.guardar_estacion("WS_TEST", "Pruebas", "DESCONECTADA")
        BD.modificar_estados("WS_TEST", "DISPONIBLE")

        """SOCKETS"""
        hilo_sockets = threading.Thread(target=servidor_sockets, args=(config.socket_port,), daemon = True)
        hilo_sockets.start()

        #Iniciar el consumidor de solicitudes
        hilo_kafka = threading.Thread(target=escuchar_solicitudes_riego, args=(consumidor, detener), daemon=True)
        hilo_kafka.start()

        print("[CENTRAL] Servidor iniciado.")

        while True:
            detener.wait(timeout=1)
    
    except KeyboardInterrupt:
        print("\n[CENTRAL] Cerrando...")

    finally:
        detener.set()

        if hilo_kafka is not None:
            hilo_kafka.join(timeout=3)

        if productor is not None:
            productor.close(timeout=5)

        print("[CENTRAL] Cerrada.")

if __name__ == "__main__":
   central()