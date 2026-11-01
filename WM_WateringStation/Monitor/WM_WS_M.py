import socket
import sys
import time
import threading
from WM_WateringStation.Monitor.ui import MonitorGUI

FORMAT = 'utf-8'

gui = None
fuera_servicio = threading.Event()
estado_engine_actual = "DESCONECTADA"

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
                log ("[MONITOR] Conexion cerrada por la central")
                gui.publicar_estado("DESCONECTADO")
                break

            for linea in msg.split("\n"):
                linea = linea.strip()
                if not linea:
                    continue

                partes = linea.split('#')
                orden = partes[0]    

                if orden == "OUT_OF_ORDER":
                    fuera_servicio.set()
                    log(f"[MONITOR] Orden recibida de central: Estacion fuera de servicio.") #Naranja
                    gui.publicar_estado("OUT_OF_ORDER")
                    send(cliente, f"ORDEN_OoO_RECIBIDO#{id_estacion}")

                elif orden == "ACTIVACION":
                    fuera_servicio.clear()
                    log(f"[MONITOR] Orden recibida de central: Estacion activada.") #Verde
                    if estado_engine_actual == "OK":
                        gui.publicar_componentes("OK", "OK")
                        gui.publicar_estado("DISPONIBLE")
                    else:
                        gui.publicar_componentes("OK", "KO")
                        gui.publicar_estado("AVERIADO")
                    
                    send(cliente, f"ORDEN_ACTIVACION_RECIBIDA#{id_estacion}")

                elif orden in ["AVISO_FUGA_RECIBIDO", "FUGA_RESUELTA_CONFIRMACION"]:
                    log(f"[MONITOR] Confirmacion: {linea}")
        except (ConnectionError, OSError) as error:
            log(f"[MONITOR] Conexion con Central perdida: {error}")
            gui.publicar_estado("DESCONECTADO")
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
                log(
                    f"[MONITOR][DESCONECTADA] No se puede conectar "
                    f"al Engine de {id_estacion}.", flush=True
                )
                return "DESCONECTADA", None

            send(nuevo_socket, f"CONEXION_ENGINE#{id_estacion}")
            respuesta = recibir_msg(nuevo_socket).strip()

            if respuesta != f"CONEXION_OK#{id_estacion}":
                nuevo_socket.close()
                log(
                    f"[MONITOR][DESCONECTADA] Resupuesta de conexión "
                    f"inválida: {respuesta}", flush=True
                )
                return "DESCONECTADA", None

            socket_engine = nuevo_socket
            log(f"[MONITOR][OK] Conectado al Engine de {id_estacion}.", flush=True)

        #comprobar que el engine sigue vivo con tiempo
        send(socket_engine, f"COMPROBACION_ESTADO_WS#{id_estacion}")
        respuesta = recibir_msg(socket_engine).strip()

        if respuesta == f"ESTADO_OK#{id_estacion}":
            return "OK", socket_engine
        elif respuesta == f"KO#{id_estacion}":
            log(f"[MONITOR][FUGA] Engine de {id_estacion} responde KO.", flush=True)
            return "FUGA", socket_engine

        log(
            f"[MONITOR][DESCONECTADA] Respuesta inesperada: "
            f"{respuesta}", flush=True
        )
        socket_engine.close()
        return "DESCONECTADA", None

    except (socket.timeout, ConnectionRefusedError, OSError) as error:
        log(
            f"[MONITOR][DESCONECTADA] Error comunicado con {id_estacion}: {error}", flush=True
        )
        if socket_engine is not None:
            socket_engine.close()
        return "DESCONECTADA", None    

def conectar_central(ip, puerto, id_estacion, ubicacion):
    """Intenta conectar y registrar el Monitor en Central."""

    while True:
        cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cliente.settimeout(5)

        try:
            cliente.connect((ip, puerto))

            log(f"[MONITOR] Conectado a Central en {ip}:{puerto}")

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

            log(f"[MONITOR] Estación {id_estacion} registrada.")

            # Autenticar la conexión.
            send(cliente, f"CONEXION#{id_estacion}")
            respuesta = recibir_msg(cliente)

            if respuesta != f"CONEXION_OK#{id_estacion}":
                raise ConnectionError(
                    f"Autenticación rechazada: {respuesta}"
                )

            log("[MONITOR] Autenticación con Central completada.")

            # Restaurar el modo bloqueante para escuchar mensajes.
            cliente.settimeout(None)

            return cliente

        except (ConnectionError, OSError, socket.timeout) as error:
            log(
                f"[MONITOR][CENTRAL DESCONECTADA] "
                f"No se puede conectar con Central: {error}"
            )

            cliente.close()
            time.sleep(3)

def supervisar_engine(ip_engine, puerto_engine, id_estacion, detener):
    """Supervisa el Engine independientemente de la conexión con Central."""
    global estado_engine_actual

    socket_engine = None
    incidencia = None

    while not detener.is_set():
        estado_engine, socket_engine = comprobar_engine(
            ip_engine,
            puerto_engine,
            id_estacion,
            socket_engine
        )

        estado_engine_actual = estado_engine

        if estado_engine == "FUGA":
            if incidencia != "FUGA":
                incidencia = "FUGA"
                log(
                    f"[MONITOR][FUGA] Fuga detectada en {id_estacion}."
                )

            gui.publicar_componentes("OK", "KO")

            if not fuera_servicio.is_set():
                gui.publicar_estado("AVERIADO")

        elif estado_engine == "DESCONECTADA":
            if incidencia != "AVERIADA":
                incidencia = "AVERIADA"
                log(
                    f"[MONITOR][AVERIADA] Engine de {id_estacion} "
                    "no disponible."
                )

            gui.publicar_componentes("OK", "KO")

            if not fuera_servicio.is_set():
                gui.publicar_estado("AVERIADO")

        elif estado_engine == "OK":
            if incidencia == "FUGA":
                log(f"[MONITOR] Fuga resuelta en {id_estacion}.")

            elif incidencia == "AVERIADA":
                log(f"[MONITOR] Engine recuperado en {id_estacion}.")

            gui.publicar_componentes("OK", "OK")

            if not fuera_servicio.is_set():
                gui.publicar_estado("DISPONIBLE")

            incidencia = None

        detener.wait(1)

    if socket_engine is not None:
        socket_engine.close()

def log(*args, **kwargs):
    print(*args, **kwargs)

    mensaje = " ".join(str(arg) for arg in args)

    if gui is not None:
        gui.publicar_mensaje(mensaje)


def ejecutar_monitor():
    global estado_engine_actual

    if len(sys.argv) != 7:
        log(
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

        # La conexión con Central puede tardar.
        # El hilo del Engine sigue funcionando mientras tanto.
        cliente = conectar_central(
            SERVER_CENTRAL,
            PORT_CENTRAL,
            ID_Estacion,
            Ubicacion
        )

        hilo_central = threading.Thread(
            target=escuchar_central,
            args=(cliente, ID_Estacion),
            daemon=True
        )

        hilo_central.start()

        while not detener.is_set():
            time.sleep(0.5)

    except KeyboardInterrupt:
        log("\nCerrando Monitor.")

    except Exception as error:
        log(f"[MONITOR] Error inesperado: {error}")

    finally:
        detener.set()

        if hilo_engine.is_alive():
            hilo_engine.join(timeout=3)

        if cliente is not None:
            cliente.close()

def main():
    global gui

    gui = MonitorGUI(
        sys.argv[5],
        sys.argv[6]
    )

    hilo_monitor = threading.Thread(
        target=ejecutar_monitor,
        daemon=True
    )

    hilo_monitor.start()

    gui.ejecutar()

if __name__ == "__main__":
    main()
