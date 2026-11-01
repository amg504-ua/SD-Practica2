import socket
import sys
import time
import threading

FORMAT = 'utf-8'

#gui = None
fuera_servicio = threading.Event()
estado_engine_actual = "DESCONECTADA"
estado_riego_actual = None

"""SOCKETS"""

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
    return msg.strip()

def escuchar_central(cliente, id_estacion):
    #global estacion_bloqueada
    while True:
        try:
            msg = recibir_msg(cliente)
            if not msg:
                print ("[MONITOR] Conexion cerrada por la central")
                break

            for linea in msg.split("\n"):
                linea = linea.strip()
                if not linea:
                    continue

                partes = linea.split('#')
                orden = partes[0]    

                if orden == "OUT_OF_ORDER":
                    fuera_servicio.set()
                    print(f"[MONITOR] Orden recibida de central: Estacion fuera de servicio.") #Naranja
                    send(cliente, f"ORDEN_OoO_RECIBIDO#{id_estacion}")

                elif orden == "ACTIVACION":
                    fuera_servicio.clear()
                    print(f"[MONITOR] Orden recibida de central: Estacion activada.") #Verde
                    send(cliente, f"ORDEN_ACTIVACION_RECIBIDA#{id_estacion}")

                elif orden in ["AVISO_FUGA_RECIBIDO", "FUGA_RESUELTA_CONFIRMACION"]:
                    print(f"[MONITOR] Confirmacion: {linea}")
        except (ConnectionError, OSError) as error:
            print(f"[MONITOR] Conexion con Central perdida: {error}")
            break

#funcion para preguntar al engine cada segundo
def comprobar_engine(ip_engine, puerto_engine, id_estacion, socket_engine):
    #global estacion_bloqueada
    #if estacion_bloqueada:
    #    return "FUERA_DE_SERVICIO", socket_engine
    try:
        if socket_engine is None:
            #crear el socket y conectarse, ademas del timeout de dos segundos, si tarda mas fuera
            nuevo_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            nuevo_socket.settimeout(2)

            try:
                nuevo_socket.connect((ip_engine, puerto_engine))
            except (socket.timeout, ConnectionRefusedError, OSError):
                nuevo_socket.close()
                print(
                    f"[MONITOR][DESCONECTADA] No se puede conectar "
                    f"al Engine de {id_estacion}.", flush=True
                )
                return "DESCONECTADA", None, None

            send(nuevo_socket, f"CONEXION_ENGINE#{id_estacion}")
            respuesta = recibir_msg(nuevo_socket).strip()

            if respuesta != f"CONEXION_OK#{id_estacion}":
                nuevo_socket.close()
                print(
                    f"[MONITOR][DESCONECTADA] Resupuesta de conexión "
                    f"inválida: {respuesta}", flush=True
                )
                return "DESCONECTADA", None, None

            socket_engine = nuevo_socket
            print(f"[MONITOR][OK] Conectado al Engine de {id_estacion}.", flush=True)

        #comprobar que el engine sigue vivo con tiempo
        send(socket_engine, f"COMPROBACION_ESTADO_WS#{id_estacion}")
        respuesta = recibir_msg(socket_engine).strip()

        if respuesta.startswith(f"ESTADO_OK#{id_estacion}#"):
            partes = respuesta.split("#")
            estado_riego = partes[2]
            return "OK", socket_engine, estado_riego
        elif respuesta == f"KO#{id_estacion}":
            print(f"[MONITOR][FUGA] Engine de {id_estacion} responde KO.", flush=True)
            return "FUGA", socket_engine, None

        print(
            f"[MONITOR][DESCONECTADA] Respuesta inesperada: "
            f"{respuesta}", flush=True
        )
        socket_engine.close()
        return "DESCONECTADA", None, None

    except (socket.timeout, ConnectionRefusedError, OSError) as error:
        print(
            f"[MONITOR][DESCONECTADA] Error comunicado con {id_estacion}: {error}", flush=True
        )
        if socket_engine is not None:
            socket_engine.close()
        return "DESCONECTADA", None, None    

def conectar_central(ip, puerto, id_estacion, ubicacion):
    """Intenta conectar y registrar el Monitor en Central."""

    while True:
        cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cliente.settimeout(5)

        try:
            cliente.connect((ip, puerto))

            print(f"[MONITOR] Conectado a Central en {ip}:{puerto}")

            # Registrar la estación.
            send(cliente, f"REGISTRO#{id_estacion}#{ubicacion}")
            respuesta = recibir_msg(cliente)

            if respuesta not in [
                f"REGISTRO_OK#{id_estacion}",
                f"REGISTRO_EXISTENTE#{id_estacion}"
            ]:
                raise ConnectionError(
                    f"Registro rechazado: {respuesta}"
                )

            print(f"[MONITOR] Estación {id_estacion} registrada.")

            # Autenticar la conexión.
            send(cliente, f"CONEXION#{id_estacion}")
            respuesta = recibir_msg(cliente)

            if respuesta != f"CONEXION_OK#{id_estacion}":
                raise ConnectionError(
                    f"Autenticación rechazada: {respuesta}"
                )

            print("[MONITOR] Autenticación con Central completada.")

            # Restaurar el modo bloqueante para escuchar mensajes.
            cliente.settimeout(None)

            return cliente

        except (ConnectionError, OSError, socket.timeout) as error:
            print(
                f"[MONITOR][CENTRAL DESCONECTADA] "
                f"No se puede conectar con Central: {error}"
            )

            cliente.close()
            time.sleep(3)

def enviar_estado_actual(cliente, id_estacion):
    estado = estado_riego_actual

    if estado is None:
        print(
            f"[MONITOR][AVISO] No se puede comunicar "
            f"el estado real de {id_estacion}."
        )
        return False

    send(
        cliente,
        f"ESTADO_ACTUAL#{id_estacion}#{estado}"
    )

    respuesta = recibir_msg(cliente)

    if respuesta == f"ESTADO_ACTUAL_OK#{id_estacion}":
        print(
            f"[MONITOR] Estado actual comunicado a Central: "
            f"{estado}."
        )
        return True

    print(
        f"[MONITOR][ERROR] Central no confirmó el estado: "
        f"{respuesta}"
    )
    return False

def supervisar_engine(ip_engine, puerto_engine, id_estacion, detener):
    """Supervisa el Engine independientemente de la conexión con Central."""
    global estado_engine_actual, estado_riego_actual

    socket_engine = None
    incidencia = None

    while not detener.is_set():
        estado_engine, socket_engine, estado_riego = comprobar_engine(
            ip_engine,
            puerto_engine,
            id_estacion,
            socket_engine
        )

        estado_engine_actual = estado_engine

        if estado_engine == "OK":
            estado_riego_actual = estado_riego
        else:
            estado_riego_actual = None

        if estado_engine == "FUGA":
            if incidencia != "FUGA":
                incidencia = "FUGA"
                print(
                    f"[MONITOR][FUGA] Fuga detectada en {id_estacion}."
                )

        elif estado_engine == "DESCONECTADA":
            if incidencia != "AVERIADA":
                incidencia = "AVERIADA"
                print(
                    f"[MONITOR][AVERIADA] Engine de {id_estacion} "
                    "no disponible."
                )

        elif estado_engine == "OK":
            if incidencia == "FUGA":
                print(f"[MONITOR] Fuga resuelta en {id_estacion}.")

            elif incidencia == "AVERIADA":
                print(f"[MONITOR] Engine recuperado en {id_estacion}.")

            incidencia = None

        detener.wait(1)

    if socket_engine is not None:
        socket_engine.close()

def main():
    global estado_engine_actual

    if len(sys.argv) != 7:
        print(
            "Argumentos equivocados. Necesito: "
            "ip_central, puerto_central, ip_engine, "
            "puerto_engine, id_estacion, ubicacion"
        )
        return

    SERVER_CENTRAL = sys.argv[1]
    PORT_CENTRAL = int(sys.argv[2])

    SERVER_ENGINE = sys.argv[3]
    PORT_ENGINE = int(sys.argv[4])

    ID_Estacion = sys.argv[5]
    Ubicacion = sys.argv[6]

    cliente = None
    detener = threading.Event()

    # El Engine se supervisa independientemente de Central.
    hilo_engine = threading.Thread(
        target=supervisar_engine,
        args=(
            SERVER_ENGINE,
            PORT_ENGINE,
            ID_Estacion,
            detener
        ),
        name="hilo-engine",
        daemon=True
    )

    try:
        hilo_engine.start()
        while  not detener.is_set():
            # La conexión con Central puede tardar.
            # El hilo del Engine sigue funcionando mientras tanto.
            cliente = conectar_central(
                SERVER_CENTRAL,
                PORT_CENTRAL,
                ID_Estacion,
                Ubicacion
            )

            tiempo_espera = 0

            while estado_riego_actual is None and tiempo_espera < 5 and not detener.is_set():
                time.sleep(0.2)
                tiempo_espera += 0.2

            if estado_riego_actual is not None:
                enviar_estado_actual(cliente, ID_Estacion)
            else:
                print("[MONITOR] No se ha podido obtener el estado real del riego. ", flush=True)

            hilo_central = threading.Thread(
                target=escuchar_central,
                args=(cliente, ID_Estacion),
                daemon=True
            )

            hilo_central.start()

            hilo_central.join()
            cliente.close()

            if not detener.is_set():
                print("[Monitor] Intentando recuperar la conexión con Central...")
                detener.wait(3)
                
        while not detener.is_set():
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\nCerrando Monitor.")

    except Exception as error:
        print(f"[MONITOR] Error inesperado: {error}")

    finally:
        detener.set()

        if hilo_engine.is_alive():
            hilo_engine.join(timeout=3)

        if cliente is not None:
            cliente.close()

if __name__ == "__main__":
    main()
