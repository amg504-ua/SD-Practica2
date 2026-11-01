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

def log(*args, **kwargs):
    print(*args, **kwargs)

    mensaje = " ".join(str(arg) for arg in args)

    if gui is not None:
        gui.publicar_mensaje(mensaje)

def ejecutar_monitor():
    global estado_engine_actual

    if len(sys.argv) == 7:
        SERVER_CENTRAL = sys.argv[1]
        PORT_CENTRAL = int(sys.argv[2])
        ADDR_CENTRAL = (SERVER_CENTRAL, PORT_CENTRAL)

        SERVER_ENGINE = sys.argv[3]
        PORT_ENGINE = int(sys.argv[4])

        ID_Estacion = sys.argv[5]
        Ubicacion = sys.argv[6]

        try:

            #creamos el socket
            cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            cliente.connect(ADDR_CENTRAL)#establecer conexion
            log (f"Establecida conexión en [{ADDR_CENTRAL}]")

            mensaje = f"REGISTRO#{ID_Estacion}#{Ubicacion}"
            send(cliente, mensaje)

            respuesta = recibir_msg(cliente)

            if respuesta == f"REGISTRO_OK#{ID_Estacion}":
                log(f"[MONITOR] Estación {ID_Estacion} registrada correctamente.")

            elif respuesta == f"REGISTRO_EXISTENTE#{ID_Estacion}":
                log(f"[MONITOR] Estación {ID_Estacion} ya registrada. Reconectando...")

            else:
                log(f"[MONITOR] Registro denegado: {respuesta}")
                cliente.close()#cerrar
                sys.exit(1)

            #enviar la conexion
            msg = f"CONEXION#{ID_Estacion}"
            log("Envio al servidor: ", msg)
            send(cliente, msg)
            respuesta_conexion = recibir_msg(cliente).strip()
            log(f"Recibido: {respuesta_conexion}")

            if respuesta_conexion != f"CONEXION_OK#{ID_Estacion}":
                log("Conexion denegada")
                cliente.close()
                sys.exit(1)

            hilo_central = threading.Thread(target= escuchar_central, args=(cliente, ID_Estacion), daemon=True)
            hilo_central.start()

            socket_engine = None
            incidencia = False

            while True:
                estado_engine, socket_engine = comprobar_engine(SERVER_ENGINE, PORT_ENGINE, ID_Estacion, socket_engine)

                estado_engine_actual = estado_engine

                if estado_engine == "FUGA":
                    if incidencia != "FUGA":
                        incidencia = "FUGA"
                        log(
                            f"[MONITOR][FUGA] Fuga detectada en "
                            f"{ID_Estacion}. Avusando a Central.", flush=True
                        )
                        send(cliente, f"AVISO_FUGA#{ID_Estacion}")
                        gui.publicar_componentes("OK", "KO")

                        if not fuera_servicio.is_set():
                            gui.publicar_estado("AVERIADO")


                elif estado_engine == "DESCONECTADA":
                    if incidencia != "AVERIADA":
                        incidencia = "AVERIADA"
                        log(
                            f"[MONITOR][AVERIADA] Engine de "
                            f"{ID_Estacion} averiado. Avisando a Central.", flush=True
                        )
                        send(cliente, f"AVISO_AVERIA#{ID_Estacion}")

                        gui.publicar_componentes("OK", "KO")

                        if not fuera_servicio.is_set():
                            gui.publicar_estado("AVERIADO")
                    
                elif estado_engine == "OK":
                    if incidencia == "FUGA":
                        log(
                            f"[MONITOR] Fuga resuelta en {ID_Estacion}.",
                            flush=True
                        )
                        send(cliente, f"FUGA_RESUELTA#{ID_Estacion}")

                    elif incidencia == "AVERIADA":
                        log(
                            f"[MONITOR] Engine recuperado en {ID_Estacion}.",
                            flush=True
                        )
                        send(cliente, f"AVERIA_RESUELTA#{ID_Estacion}")
                    
                    gui.publicar_componentes("OK", "OK")

                    if not fuera_servicio.is_set():
                        gui.publicar_estado("DISPONIBLE")
                    incidencia = None

                time.sleep(1)

        except ConnectionRefusedError:
            log(f"Conexion rexhazada")
        except KeyboardInterrupt:
            log("\nCerrando Monitor")
            cliente.close()
    else:
        log("argumentos equivocados, necesito: Ip_central, puerto_central, ip_engine, puerto_engine, id_estacion, ubicacion")

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
