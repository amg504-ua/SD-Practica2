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

#un hilo por cada monitor que se conecta
def handle_client(conn, addr, productor_kafka):
    print(f"[NUEVA CONEXION] {addr} connected.")
    #variable para guardar la estacion en la bd
    id_estacionA = None 
    
    connected = True
    while connected:
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
                        if BD.existe_estacion(id_estacion):
                            respuesta = f"REGISTRO_EXISTENTE#{id_estacion}"
                        else:    
                            #guardar en la bd
                            BD.guardar_estacion(id_estacion, ubicacion)
                            print(f"[REGISTRO] Nueva estación registrada: "
                                  f"{id_estacion} en {ubicacion}.")
                            respuesta = f"REGISTRO_OK#{id_estacion}"
                    
                    #si la estacion se quiere conectar y ya esta registrada
                    elif tipo == "CONEXION" and len(partes) == 2:
                        id_estacion = partes[1]
                        id_estacionA = id_estacion
                        BD.modificar_estados(id_estacion, "DISPONIBLE")
                        estaciones = BD.devolver_estaciones()
                        ubicacion = next(
                            (e[1] for e in estaciones if e[0] == id_estacion),"N/D"
                        )
                        enviar_estado_fo(
                            productor_kafka,
                            id_estacion,
                            "DISPONIBLE",
                            ubicacion
                        )
                        print(f"Estacion disponible: {id_estacion}")
                        respuesta = f"CONEXION_OK#{id_estacion}"

                    #el monitor no se puede conectar al engine pq este se ha caido
                    elif tipo == "AVISO_FUGA" and len(partes) == 2:
                        id_estacion = partes[1]
                        BD.modificar_estados(id_estacion, "FUGA")
                        estaciones = BD.devolver_estaciones()
                        ubicacion = next(
                            (e[1] for e in estaciones if e[0] == id_estacion),"N/D"
                        )
                        enviar_estado_fo(
                            productor_kafka,
                            id_estacion,
                            "FUGA",
                            ubicacion
                        )
                        print(f"Estacion en fuga: {id_estacion}")
                        respuesta = f"AVISO_FUGA_RECIBIDO#{id_estacion}"
                    
                    #sel engine ya esta operativo
                    elif tipo == "FUGA_RESUELTA" and len(partes) == 2:
                        id_estacion = partes[1]
                        BD.modificar_estados(id_estacion, "DISPONIBLE")
                        estaciones = BD.devolver_estaciones()
                        ubicacion = next(
                            (e[1] for e in estaciones if e[0] == id_estacion),"N/D"
                        )
                        enviar_estado_fo(
                            productor_kafka,
                            id_estacion,
                            "DISPONIBLE",
                            ubicacion
                        )
                        print(f"Estacion CON FUGA RESUELTA: {id_estacion}")
                        respuesta = f"FUGA_RESUELTA_CONFIRMACION#{id_estacion}"
                    
                    elif tipo in ["ORDEN_OoO_RECIBIDO", "ORDEN_ACTIVACION_RECIBIDA"]: 
                        print(f"Estacion disponible: {id_estacion}")
                        respuesta = f"CONEXION_OK#{id_estacion}"

                    else:
                        respuesta = F"ERROR#Formato incorrecto {msg}"

                    conn.send((respuesta + "\n").encode(FORMAT))
            else:
                #pasa cuando recv() esta vacio, el cliente cierra la conexion
                print(f"DESCONEXION DEL MONITOR EN {addr}")
                connected = False

        except (ConnectionError, OSError) as exc:
            print(f"[ERROR DE CONEXIÓN] {addr}: {exc}", flush=True)

    if id_estacionA is not None:
        BD.modificar_estados(id_estacionA, "DESCONECTADA")
        estaciones = BD.devolver_estaciones()
        ubicacion = next(
            (e[1] for e in estaciones if e[0] == id_estacion),"N/D"
        )
        enviar_estado_fo(
            productor_kafka,
            id_estacionA,
            "DESCONECTADA",
            ubicacion
        )
        print(f"[ESTACIÓN] {id_estacionA} pasa a DESCONECTADA.", flush=True)

    
    conn.close()
    print(f"Conecion cerrada {addr}", flush=True)

def servidor_sockets(puerto, productor):
    SERVER = socket.gethostbyname(socket.gethostname())
    ADDR = (SERVER, puerto)
    #se pone en 0.0.0.0 para escuchar en las interfaces de la red
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #evita el error de que ya esta en uso.
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
                args=(conn, addr, productor),
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

def enviar_estado_fo(productor, id_estacion, estado, ubicacion, caudal="-", volumen="-", id_operario="-"):
    
    mensaje = (f"{id_estacion}#{estado}#{ubicacion}#{caudal}#{volumen}#{id_operario}")

    try:
        resultado = productor.send("ce.estados.fo", value=mensaje).get(timeout=10)
        print(f"[KAFKA] Estado enviado: {mensaje} (Topic: {resultado.topic})", flush=True)
    
    except Exception as error:
        print(f"[KAFKA] Error al enviar el estado: {error}", flush=True)

def enviar_evento_fo(productor, mensaje):

    try:
        resultado = productor.send(
            "ce.eventos.fo",
            value=mensaje
        ).get(timeout=10)
        print(
            f"[KAFKA] Evento enviado a FO: {mensaje} "
            f"(Topic: {resultado.topic})",
            flush=True
        )
    except Exception as error:
        print(f"[KAFKA] Error enviado evento a FO: {error}", flush=True) 

def enviar_orden_engine(productor, solicitud):

    mensaje = (
        f"{solicitud['id_peticion']}#ORDEN_RIEGO#"
        f"{solicitud['id_operario']}#"
        f"{solicitud['id_estacion']}#"
        f"{solicitud['tiempo_riego']}"
    )

    try:
        resultado = productor.send("ce.ordenes.e", value=mensaje).get(timeout=10)

        print(f"[KAFKA] Orden enviada al Engine: {mensaje} (Topic: {resultado.topic})", flush=True)

        return True

    except Exception as error:
        print(f"[KAFKA] Error enviando la orden al Engine: {error}", flush=True)
        
        return False
#======= CONSUMIDORES =======#

def crear_consumidor_kafka(ip, puerto, topic):
    consumidor = KafkaConsumer(
        topic,
        bootstrap_servers=f"{ip}:{puerto}",
        group_id=f"wm-central-{topic.replace('.', '-')}",
        auto_offset_reset="latest",
        value_deserializer=lambda mensaje: mensaje.decode("utf-8")
    )

    return consumidor

def escuchar_solicitudes_riego(consumidor, productor, detener):
    try:
        print("[KAFKA] Escuchando solicitudes de riego...")

        while not detener.is_set():
            mensajes = consumidor.poll(timeout_ms=500)

            for registros in mensajes.values():
                for mensaje in registros:
                    contenido = mensaje.value

                    print(f"[SOLICITUD RECIBIDA] {mensaje.value}")
                    
                    campos = contenido.split("#")

                    valida, motivo, solicitud = validar_solicitud_riego(contenido)

                    if len(campos) != 5:
                        print(
                            "[SOLICITUD DENEGADA] Formato incorrecto; "
                            "no se puede construir una respuesta completa.",
                            flush=True
                        )
                        continue

                    id_peticion, tipo, id_operario, id_estacion, tiempo = campos

                    if valida:
                        evento = (
                            f"{id_peticion}#RIEGO_AUTORIZADO#"
                            f"{id_operario}#{id_estacion}"
                        )

                        print(
                            f"[VALIDACIÓN OK] Petición {id_peticion}: "
                            f"Estación: {id_estacion}",
                            flush=True
                        )
                    else:
                        evento = (
                            f"{id_peticion}#RIEGO_DENEGADO#"
                            f"{id_operario}#{id_estacion}#{motivo}"
                        )
                        print(f"[SOLICITUD DENEGADA] {motivo}", flush=True)
                    
                    enviar_evento_fo(productor, evento)

                    if valida:
                        enviado = enviar_orden_engine(productor, solicitud)

                        if not enviado:
                            print(
                                f"[ERROR] No se pudo enviar la orden de riego "
                                f"{solicitud['id_peticion']}",
                                flush=True
                            )
    finally:
        consumidor.close()

def validar_solicitud_riego(mensaje):

    campos = mensaje.split("#")

    if len(campos) != 5:
        return False, "Formato de solicitud incorrecto.", None

    id_peticion, tipo, id_operario, id_estacion, tiempo = campos

    if tipo != "SOLICITUD_RIEGO":
        return False, "Tipo de solicitud desconocido.", None
    if not id_peticion or not id_operario or not id_estacion:
        return False, "Faltan campos obligatorios.", None

    try:
        tiempo_riego = int(tiempo)
        if tiempo_riego <= 0:
            raise ValueError
    except ValueError:
        return False, "La duración debe ser un entero positivo.", None

    estaciones = BD.devolver_estaciones()

    estacion = next(
        (
            e for e in estaciones
            if e[0] == id_estacion
        ), None
    )

    if estacion is None:
        return False, f"Estación {id_estacion} no registrada.", None

    estado = estacion[2]

    if estado != "DISPONIBLE":
        return False, (
            f"Estación {id_estacion} no disponible."
            f"(Estado: {estado})"
        ), None

    solicitud = {
        "id_peticion": id_peticion,
        "id_operario": id_operario,
        "id_estacion": id_estacion,
        "tiempo_riego": tiempo_riego,
    }

    return True, "Solicitud validada correctamente.", solicitud

def escuchar_eventos_engine(consumidor, productor, detener):
    print("[KAFKA] Escuchando eventos del Engine...", flush=True)

    try:
        while not detener.is_set():
            mensajes = consumidor.poll(timeout_ms=500)

            for registros in mensajes.values():
                for mensaje in registros:
                    contenido = mensaje.value
                    campos = contenido.split("#")

                    if len(campos) < 2:
                        print(
                            f"[ENGINE] Evento descartado: Formato incorrecto: {contenido}",
                            flush=True
                        )
                        continue

                    tipo = campos[1]

                    if tipo == "RIEGO_INTERRUMPIDO":
                        if len(campos) != 5:
                            print(
                                f"[ENGINE] Intrrupción descartada: Formato Incorrecto: "
                                f"{contenido}",
                                flush=True
                            )
                            continue

                        print(f"[CENTRAL] {contenido}", flush=True)
                        enviar_evento_fo(productor, contenido)
                        continue

                    if tipo in ("DATOS_RIEGO", "RESUMEN_RIEGO"):
                        if len(campos) != 6:
                            print(
                                f"[ENGINE] Evento descartado: Formato incorrecto: {contenido}", flush=True
                            )
                            continue
                    else:
                        print(f"[ENGINE] Evento desconocido: {tipo}", flush=True)
                        continue

                    print(f"[ENGINE] Evento recibido: {contenido}", flush=True)
                    enviar_evento_fo(productor, contenido)
    
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

    #ESTACIÓN CREADA PRUEBAS
    #BD.guardar_estacion("WS_TEST", "Pruebas", "DESCONECTADA")
    #BD.modificar_estados("WS_TEST", "DISPONIBLE")

    estaciones = BD.devolver_estaciones()

    for estacion in estaciones:
        print(f"[ESTACIÓN] : {estacion[0]}, [UBICACIÓN] : {estacion[1]}, [ESTADO] : {estacion[2]} .")

    try:
    
        """KAFKA"""
        consumidor = crear_consumidor_kafka(config.kafka_host, config.kafka_port, "fo.peticion_riego.ce")
        consumidor_engine = crear_consumidor_kafka(config.kafka_host, config.kafka_port, "e.eventos.ce")
        productor = crear_productor_kafka(config.kafka_host, config.kafka_port)

        for estacion in BD.devolver_estaciones():
            id_estacion, ubicacion, estado = estacion
            enviar_estado_fo(productor, id_estacion, estado, ubicacion)            

        print("[CENTRAL] Conexiones Kafka creadas.")

        """SOCKETS"""
        hilo_sockets = threading.Thread(target=servidor_sockets, args=(config.socket_port, productor), daemon = True)
        hilo_sockets.start()

        #Iniciar el consumidor de solicitudes
        hilo_kafka = threading.Thread(target=escuchar_solicitudes_riego, args=(consumidor, productor, detener), daemon=True)
        hilo_kafka.start()

        hilo_kafka_engine = threading.Thread(target=escuchar_eventos_engine, args=(consumidor_engine, productor, detener), daemon=True)
        hilo_kafka_engine.start()

        print("[CENTRAL] Servidor iniciado.")

        while True:
            detener.wait(timeout=1)
    
    except KeyboardInterrupt:
        print("\n[CENTRAL] Cerrando...")

    finally:
        detener.set()

        if hilo_kafka is not None:
            hilo_kafka.join(timeout=3)

        if hilo_kafka_engine is not None:
            hilo_kafka_engine.join(timeout=3) 

        if productor is not None:
            productor.close(timeout=5)

        print("[CENTRAL] Cerrada.")

if __name__ == "__main__":
   central()