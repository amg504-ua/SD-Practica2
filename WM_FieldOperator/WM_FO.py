import argparse
import threading
import uuid
from Common.config import load_field_operator_config
from kafka import KafkaConsumer, KafkaProducer

estaciones = {}
bloqueo_estaciones = threading.Lock()
solicitud_activa = None
bloqueo_solicitud = threading.Lock()

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

def escuchar_eventos(consumidor, detener, id_operador):    
    """Interpreta los eventos recibidos desde WM_Central"""

    formatos = {
        "RIEGO_AUTORIZADO": 4,
        "RIEGO_DENEGADO": 5,
        "DATOS_RIEGO": 6,
        "RESUMEN_RIEGO": 6,
        "RIEGO_INTERRUMPIDO": 5,
    }

    try:
        while not detener.is_set():
            mensajes = consumidor.poll(timeout_ms=500)

            for registros in mensajes.values():
                for mensaje in registros:
                    campos = mensaje.value.split("#")

                    if len(campos) < 4:
                        print("\n[Error] Mensaje de Central no válido.")
                        continue

                    id_peticion = campos[0]
                    tipo_evento = campos[1]

                    if tipo_evento not in formatos:
                        print(f"\n[Error] Evento desconocido: {tipo_evento}")
                        continue

                    if len(campos) != formatos[tipo_evento]:
                        print(f"\n[Error] Formato incorrecto: {mensaje.value}")
                        continue

                    id_operario_evento = campos[2]
                    id_estacion = campos[3]

                    if id_operario_evento != str(id_operador):
                        continue

                #GESTIONAMOS LOS TIPOS DE EVENTOS
                
                    if tipo_evento == "RIEGO_AUTORIZADO":
                        print(
                            f"\n[Central] Riego Autorizado. "
                            f"Petición: {id_peticion}. "
                            f"Estación: {id_estacion}."
                        )
                    elif tipo_evento == "RIEGO_DENEGADO":
                        motivo = campos[4]
                        print(
                            f"\n[Central] Riego denegado. "
                            f"Petición: {id_peticion}. "
                            f"Estación: {id_estacion}. "
                            f"Motivo: {motivo}"
                        )

                    elif tipo_evento == "DATOS_RIEGO":
                        caudal = campos[4]
                        volumen = campos[5]
                        print(
                            f"\n[Central] Evolución del riego de la petición {id_peticion} "
                            f"en la estación {id_estacion}: "
                            f"caudal = {caudal} L/min; "
                            f"volumen acumulado = {volumen} L."
                        )

                    elif tipo_evento == "RESUMEN_RIEGO":
                        tiempo = campos[4]
                        litros = campos[5]
                        print(
                            f"\n[Central] Riego finalizado "
                            f"en la estación {id_estacion}. "
                            f"Duración: {tiempo} s; "
                            f"litros suministrados: {litros} L."
                            f"Petición: {id_peticion}"
                        )

                    elif tipo_evento == "RIEGO_INTERRUMPIDO":
                        motivo = campos[4]
                        print(
                            f"\n[Central] Riego interrumpido "
                            f"en la estación {id_estacion}. "
                            f"Motivo: {motivo} "
                            f"Petición: {id_peticion}"
                        )

    finally:
        consumidor.close()

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

def gestionar_solicitud_riego(productor, id_operador):
    """Permite elegir una estación y solicitar su riego."""

    id_estacion = seleccionar_estacion()

    if id_estacion is None:
        print("Solicitud cancelada.")
        return None

    tiempo_riego = solicitar_riego()

    id_peticion = enviar_solicitud_riego(productor, id_operador, id_estacion, tiempo_riego)

    return id_peticion

#def leer_fichero(fichero):

#======= MENÚ ESTACIONES =======#

def actualizar_estacion(id_estacion, estado, ubicacion, caudal, volumen_acumulado):
    """Guarda el estado completo recibido de una estación."""

    with bloqueo_estaciones:
        estaciones[id_estacion] = {
            "estado": estado,
            "ubicacion": ubicacion,
            "caudal": caudal,
            "volumen_acumulado": volumen_acumulado,
        }

def escuchar_estados(consumidor, detener):
    """Recibe estados y actualiza las estaciones continuamente."""

    try:
        while not detener.is_set():
            mensajes = consumidor.poll(timeout_ms=500)

            for registros in mensajes.values():
                for mensaje in registros:
                    campos = mensaje.value.split("#")

                    if len(campos) != 6:
                        print("Mensaje de estado no válido.")
                        continue

                    (id_estacion, estado, ubicacion, caudal,
                     volumen, id_operario) = campos

                    try:
                        caudal = None if caudal == "-" else float(caudal)
                        volumen = None if volumen == "-" else float(volumen)
                    except ValueError:
                        print("Caudal o volumen no válido.")
                        continue

                    actualizar_estacion(id_estacion,estado,ubicacion,caudal,volumen)

                    mostrar_estaciones()

    finally:
        consumidor.close()

def mostrar_estaciones():
    """Muestra una copia de las estaciones que conocemos."""

    with bloqueo_estaciones:
        copia_estaciones = {
            id_estacion: datos.copy()
            for id_estacion, datos in estaciones.items()
        }

    if not copia_estaciones:
        print("No hay estaciones registradas.")
        return

    print("\n--- Estaciones de Riego ---")

    for id_estacion, datos in copia_estaciones.items():
        estado = datos.get("estado", "DESCONOCIDO")
        ubicacion = datos.get("ubicacion", "N/D")
        caudal = datos.get("caudal", "N/D")
        volumen = datos.get("volumen_acumulado", "N/D")

        print(
            f"{id_estacion} | {ubicacion} | {estado} | "
            f"Caudal: {caudal} | Volumen acumulado: {volumen}"
        )

def estacion_disponible(id_estacion):
    """Comprueba si una estación existe y está disponible"""

    with bloqueo_estaciones:
        datos = estaciones.get(id_estacion)

        if datos is None:
            return False

        return datos.get("estado") == "DISPONIBLE"

def seleccionar_estacion():
    """Permite al operario elegir una estación por su identificador."""

    if not estaciones:
        print("No hay estaciones disponibles para seleccionar.")
        return None

    while True:

        id_estacion = input("\nIntroduce el identificador de la estación que quieres seleccionar (o 'salir'): ").strip()

        if id_estacion.lower() == "salir":
            return None
        
        if not estacion_disponible(id_estacion):
            print("La estación no está disponible para regar.")
            continue

        return id_estacion

#======= INTERFAZ =======#

def cargar_interfaz_grafica():
    """Interfaz Gráfica del FO"""

#======= MAIN =======#

def main(id_operador, ip, puerto_kafka):
    config = load_field_operator_config()

    if ip is None:
        ip = config.kafka_host
    if puerto_kafka is None:
        puerto_kafka = config.kafka_port

    print(f"Operario {id_operador} iniciado")
    print(f"Broker Kafka: {ip}:{puerto_kafka}")

    consumidores = []
    detener = threading.Event()
    hilos = []
    productor = None

    try:
        # Creamos un consumidor para cada topic.
        consumidor_eventos = crear_consumidor_kafka(id_operador, ip, puerto_kafka, "ce.eventos.fo")
        consumidores.append(consumidor_eventos)

        consumidor_estados = crear_consumidor_kafka(id_operador, ip, puerto_kafka, "ce.estados.fo")
        consumidores.append(consumidor_estados)

        productor = crear_productor_kafka(ip, puerto_kafka)

        print("Conexiones Kafka creadas correctamente.")

        # Creamos los hilos que escucharán los topics.
        hilo_eventos = threading.Thread(target=escuchar_eventos,args=(consumidor_eventos, detener, id_operador),name="hilo-eventos")

        hilo_estados = threading.Thread(target=escuchar_estados,args=(consumidor_estados, detener),name="hilo-estados")

        hilos.extend([hilo_eventos, hilo_estados])

        # Iniciamos ambos hilos.
        for hilo in hilos:
            hilo.start()

        mostrar_estaciones() 

        print("\nEscribe 'r' para solicitar un riego o 'salir' para terminar.")

        while True:
            orden = input(f"\nWM_FO [{id_operador}] > ").strip().lower()

            if orden == "r":
                gestionar_solicitud_riego(productor, id_operador)

            elif orden == "salir":
                break

            else:
                print("Orden no válida. Utiliza 'r' o 'salir'.")

    except KeyboardInterrupt:
        print("\nInterrupción solicitada por el usuario.")

    except Exception as error:
        print(f"Error en WM_FO: {error}")

    finally:
        # Avisamos a los hilos de que deben terminar.
        detener.set()

        # Esperamos a que terminen antes de cerrar la aplicación.
        for hilo in hilos:
            if hilo.is_alive():
                hilo.join()

        if productor is not None:
            productor.close()

        print("WM_FO cerrado.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WM_FieldOperator")

    parser.add_argument("id_operador",help="Identificador del operario")
    #el -- hace que no sea un argumento obligatorio
    parser.add_argument("--ip",help="Dirección IP del broker kafka")
    parser.add_argument("--puerto_kafka",type=int ,help="Puerto del broker kafka")

    args = parser.parse_args()

    main(args.id_operador, args.ip, args.puerto_kafka)
