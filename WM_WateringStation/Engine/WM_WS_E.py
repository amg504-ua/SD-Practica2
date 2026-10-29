import socket
import sys
import time
import threading
from kafka import KafkaConsumer, KafkaProducer
from Common.config import load_engine_config

"""SOCKETS"""

FORMAT = 'utf-8'
estado = "OK" #Variable que sirve para comprobar el estado, puede ser OK o FALLO
#funcion para enviar le mensaje añadiendo el salto de linea al final
def send(socketC, msg):
    if not msg.endswith('\n'):
        msg += '\n'
    socketC.send(msg.encode(FORMAT))

def recibir_msg(socketC):
    msg = socketC.recv(2048).decode(FORMAT)
    if not msg: 
        return ""
    #la funcion striop es para quitar todos los espacios en blanco y tab del msg
    return msg.strip().split("\n")

def escuchar_monitor(conn, addr):
    print(f"Monitor conectado desde {addr}")
    conectado = True

    while conectado:
        try:
            msg = recibir_msg(conn)
            #si recibe vacio el monitor ha desconectado
            if not msg:
                conectado = False
                break

            for mensaje in msg:
                if not mensaje:
                    continue
                print(f"Mensaje del Monitor: {mensaje}")

                #procesar las ordenens del monitor
                partes = mensaje.split("#")
                orden = partes[0]

                if orden == "CONEXION_ENGINE":
                    id_estacion = partes[1]
                    print(f"Conectado con el monitor de estacion {id_estacion}")
                    send(conn, f"CONEXION_OK#{id_estacion}")

                elif orden == "COMPROBACION_ESTADO_WS":
                    id_estacion = partes[1]

                    if estado == "OK":
                        send(conn, f"ESTADO_OK#{id_estacion}")
                    else:

                        send(conn, f"KO#{id_estacion}")
                
        except (ConnectionError, OSError) as e:
            print(f"Error con el monitor: {e}")
            conectado = False
    conn.close()

def servidor_monitor(puerto, detener):
    SERVER = socket.gethostbyname(socket.gethostname())
    ADDR = (SERVER, puerto)
    #se pone en 0.0.0.0 para escuchar en las interfaces de la red
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #evita el error de que ya esta en uso.
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.settimeout(1)

    try:
        servidor.bind(("0.0.0.0", puerto))
        servidor.listen()

        while not detener.is_set():
            try:
                conn, addr = servidor.accept()
            except socket.timeout:
                continue
            except OSError:
                if detener.is_set():
                    break
                raise

            hilo = threading.Thread(
                target=escuchar_monitor,
                args=(conn, addr),
                daemon=True,
            )
            hilo.start()

    finally:
        servidor.close()
        print("[ENGINE] Servidor de sockets cerrado.", flush=True)

"""KAFKA"""

#======= PRODUCTORES =======#

def crear_productor_kafka(ip, puerto):
    return KafkaProducer(
            bootstrap_servers=f"{ip}:{puerto}",
            value_serializer=lambda mensaje: mensaje.encode("utf-8")
        )

def enviar_evento_engine(productor, mensaje):
    try:
        resultado = productor.send(
            "e.eventos.ce",
            value=mensaje
        ).get(timeout=10)

        print(
            f"[ENGINE] Evento enviado: {mensaje} "
            f"(Topic: {resultado.topic})",
            flush=True
        )
    except Exception as error:
        print(f"[ENGINE] Error enviando evento: {error}", flush=True)

#======= CONSUMIDORES =======#

def crear_consumidor_kafka(ip, puerto):
    return KafkaConsumer(
            "ce.ordenes.e",
            bootstrap_servers=f"{ip}:{puerto}",
            group_id="wm-engine-ordenes",
            #group_id=f"wm-engine-{id_estacion}",
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=lambda mensaje: mensaje.decode("utf-8")
        )
    

#funcion para simular las fugas del monitor
def menu(detener):

    global estado
    
    while not detener.is_set():
        try:
            #se hace un hilo independinete para no bloquear lo demas
            orden = input("Pulsa F para fuga o R para reparar")

        except EOFError:
            detener.set()
            break
        
        if orden == 'F':
            estado = "KO"
            print("SE ha puesto en fuga, se le comunicara al monitor")
        elif orden == 'R':
            estado = "OK"
            print("Fuga reparada se le comunicara al monitor")
        else:
            print("Tecla equivocada")

"""RIEGO"""

def ejecutar_riego(productor, id_peticion, id_operario, id_estacion, tiempo_riego):
    global estado

    print(
        f"[ENGINE] Iniciando riego: Estación {id_estacion}, "
        f"Operario {id_operario}, Duración {tiempo_riego} s.",
        flush=True
    )

    caudal = 12
    volumen_acumulado = 0.0
    tiempo_inicio = time.monotonic()
    interrumpido = False
    motivo_interrupcion = None

    try:
        for segundo in range(tiempo_riego):
            if estado != "OK":
                interrumpido = True
                motivo_interrupcion = "FUGA"
                print("[ENGINE] Riego Interrumpido por una fuga.", flush = True)
                break

            time.sleep(1)

            volumen_acumulado += caudal / 60

            mensaje = (
                f"{id_peticion}#DATOS_RIEGO#"
                f"{id_operario}#{id_estacion}#"
                f"{caudal:.2f}#{volumen_acumulado:.2f}"
            )

            enviar_evento_engine(productor, mensaje)

        if interrumpido:
            mensaje_interrupcion = (
                f"{id_peticion}#RIEGO_INTERRUMPIDO#"
                f"{id_operario}#{id_estacion}#"
                f"{motivo_interrupcion}"
            )

            enviar_evento_engine(productor, mensaje_interrupcion)

        tiempo_transcurrido = int(time.monotonic() - tiempo_inicio)

        resumen = (
            f"{id_peticion}#RESUMEN_RIEGO#"
            f"{id_operario}#{id_estacion}#"
            f"{tiempo_transcurrido}#{volumen_acumulado:.2f}"
        )

        enviar_evento_engine(productor, resumen)

        print(
            f"[ENGINE] Riego Finalizado. "
            f"Duración: {tiempo_transcurrido} s; "
            f"Volumen: {volumen_acumulado:.2f} litros",
            flush=True
        )

    finally:
        print("[ENGINE] Estación a la espera de nuevas órdenes.", flush=True)
        
def escuchar_ordenes(consumidor, productor, detener):
    print("\n[ENGINE] Escuchando órdenes de riego...", flush=True)

    try:
        while not detener.is_set():
            mensajes = consumidor.poll(timeout_ms=500)

            for registros in mensajes.values():
                for mensaje in registros:
                    contenido = mensaje.value
                    print(f"[ENGINE] Orden recibida: {contenido}", flush=True)

                    partes = contenido.split("#")

                    if len(partes) != 5:
                        print("[ENGINE] Orden descartada: Formato Incorrecto", flush=True)
                        continue

                    id_peticion, tipo, id_operario, id_estacion, tiempo = partes

                    if tipo != "ORDEN_RIEGO":
                        print(f"[ENGINE] Tipo de orden desconocido: {tipo}", flush=True)
                        continue

                    try:
                        tiempo_riego = int(tiempo)

                        if tiempo_riego <= 0:
                            raise ValueError

                    except ValueError:
                        print("[ENGINE] Duración de riego inválida.", flush=True)
                        continue

                    #if id_estacion != mi_id_estacion:
                    #    continue

                    ejecutar_riego(productor, id_peticion, id_operario, id_estacion, tiempo_riego)

    finally:
        consumidor.close()

"""MAIN"""

def main():
    print("WM_WateringStation Engine iniciado")
    
    config = load_engine_config()

    print("[ENGINE] Iniciando...", flush=True)
    print(
        f"[ENGINE] Sockets: "
        f"{config.monitor_host}:{config.monitor_port}",
        flush=True
    )
    print(
        f"[ENGINE] Kafka: "
        f"{config.kafka_host}:{config.kafka_port}",
        flush=True
    )

    if len(sys.argv) >= 2:
        puerto = int(sys.argv[1])
    else:
        puerto =  6000

    detener = threading.Event()
    consumidor = None
    productor = None
    hilo_servidor = None
    hilo_kafka = None

    try:

        consumidor = crear_consumidor_kafka(config.kafka_host, config.kafka_port)

        productor = crear_productor_kafka(config.kafka_host, config.kafka_port)

        """SOCKETS"""
        #arrancar el servidor en hilo aparte
        hilo_servidor = threading.Thread(target=servidor_monitor, args=(puerto, detener), daemon=True)
        hilo_servidor.start()

        """KAFKA"""

        hilo_kafka = threading.Thread(target=escuchar_ordenes, args=(consumidor, productor, detener), daemon=True)

        hilo_kafka.start()

        print("\n[ENGINE] Conexiones Kafka creadas.", flush=True)

        menu(detener)

    except KeyboardInterrupt:
        print("\n[ENGINE] Cerrando...", flush=True)

    finally:
        detener.set()
        if hilo_servidor is not None:
            hilo_servidor.join(timeout=2)
        if hilo_kafka is not None:
            hilo_kafka.join(timeout=3)
        if productor is not None:
            productor.close(timeout=5)
        print("[ENGINE] Cerrado.", flush=True)

if __name__ == "__main__":
    main()