import socket
import sys
import time
import threading

FORMAT = 'utf-8'
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
    while True:
        try:
            msg = recibir_msg(cliente)
            if not msg:
                print ("Conexion cerrada por la central")
                break

            for linea in msg.split('\n'):
                linea = linea.strip()
                if not linea:
                    continue

            partes = linea.split('#')
            orden = partes[0]    

            if orden == "OUT_OF_ORDER":
                print(f"\n Orden recibida de central: Estacion pasa a OUT OF SERVICE (naranja)")
                send(cliente, f"ORDEN_OoO_RECIBIDO#{id_estacion}")

            elif orden == "ACTIVACION":
                print(f"\n Orden recibida de central: Estacion reactivada, AVAILABLE (verde)")
                send(cliente, f"ORDEN_ACTIVACION_RECIBIDA#{id_estacion}")

            elif orden in ["AVISO_FUGA_RECIBIDO", "FUGA_RESUELTA_CONFIRMACION"]:
                print(f"\n Confirmacion recibida: {linea}")
        except Exception:
            print("\n conexion perdida con la central")
            break

#funcion para preguntar al engine cada segundo
def comprobar_engine(ip_engine, puerto_engine, id_estacion, socket_engine):
    try:
        if socket_engine is None:
            #crear el socket y conectarse, ademas del timeout de dos segundos, si tarda mas fuera
            nuevo_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            nuevo_socket.settimeout(2)
            nuevo_socket.connect((ip_engine, puerto_engine))

            send(nuevo_socket, f"CONEXION_ENGINE#{id_estacion}")
            respuesta = recibir_msg(nuevo_socket).strip()

            if respuesta == f"CONEXION_OK#{id_estacion}":
                print(f" Conectado correctamente al Engine: {respuesta}")
                socket_engine = nuevo_socket
            else:
                nuevo_socket.close()
                return False, None

        #comprobar que el engine sigue vivo con tiempo
        send(socket_engine, f"COMPROBACION_ESTADO_WS#{id_estacion}")
        respuesta = recibir_msg(socket_engine).strip()

        if respuesta == f"ESTADO_OK#{id_estacion}":
            return True, socket_engine
        elif respuesta == f"KO#{id_estacion}":
            print(f"Fuga detectada, engine ha respondido: {respuesta}")
            return False, socket_engine
        else:
            socket_engine.close()
            return False, None

    except (socket.timeout, ConnectionRefusedError, OSError):
        if socket_engine is not None:
            socket_engine.close()
        return False, None    


def main():

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
            print (f"Establecida conexión en [{ADDR_CENTRAL}]")

            mensaje = f"REGISTRO#{ID_Estacion}#{Ubicacion}"
            print("Envio al servidor: ", mensaje)
            send(cliente, mensaje)#enviar la orden
            respuesta = cliente.recv(2048).decode(FORMAT).strip()#esperar
            print(f"Recibido: {respuesta}")

            if respuesta != f"REGISTRO_OK#{ID_Estacion}":
                print("Registro denegado")
                cliente.close()#cerrar
                sys.exit(1)

            #enviar la conexion
            msg = f"CONEXION#{ID_Estacion}"
            print("Envio al servidor: ", msg)
            send(cliente, msg)
            respuesta_conexion = recibir_msg(cliente).strip()
            print(f"Recibido: {respuesta_conexion}")

            if respuesta_conexion != f"CONEXION_OK#{ID_Estacion}":
                print("Conexion denegada")
                cliente.close()
                sys.exit(1)

            hilo_central = threading.Thread(target= escuchar_central, args=(cliente, ID_Estacion), daemon=True)
            hilo_central.start()

            socket_engine = None
            fuga = False

            while True:
                engine_ok, socket_engine = comprobar_engine(SERVER_ENGINE, PORT_ENGINE, ID_Estacion, socket_engine)

                if not engine_ok:
                    if not fuga:
                        fuga = True
                        print(f"engine caido enviando aviso de fuga")
                        send(cliente, f"AVISO_FUGA#{ID_Estacion}")
                else:
                    if fuga:
                        fuga = False
                        print(f"Engine ya no esta en fuga enviando fuga reparada")
                        send(cliente, f"FUGA_RESUELTA#{ID_Estacion}")
                    else:
                        print(f"estacion {ID_Estacion} funcionando sin fugas")
                time.sleep(1)

        except ConnectionRefusedError:
            print(f"Conexion rexhazada")
        except KeyboardInterrupt:
            print("\nCerrando Monitor")
            cliente.close()
    else:
        print("argumentos equivocados, necesito: Ip_central, puerto_central, ip_engine, puerto_engine, id_estacion, ubicacion")

if __name__ == "__main__":
    main()
