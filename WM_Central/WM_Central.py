'''Cada paso que haga la central, deberá notificarlo al Operario.'''
import socket
import threading
import BD
import sys
import os
#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
#from Common.config import load_central_config

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
                        BD.modificar_estados(id_estacion, "Available")
                        print(f"Estacion available: {id_estacion}")
                        respuesta = f"CONEXION_OK#{id_estacion}"

                    #si la estacion se quiere conectar y ya esta registrada
                    elif tipo == "AVISO_FUGA" and len(partes) == 2:
                        id_estacion = partes[1]
                        BD.modificar_estados(id_estacion, "Leak")
                        print(f"Estacion en fuga: {id_estacion}")
                        respuesta = f"AVISO_FUGA_RECIBIDO#{id_estacion}"
                    
                    #si la estacion se quiere conectar y ya esta registrada
                    elif tipo == "FUGA_RESUELTA" and len(partes) == 2:
                        id_estacion = partes[1]
                        BD.modificar_estados(id_estacion, "Available")
                        print(f"Estacion CON FUGA RESUELTA: {id_estacion}")
                        respuesta = f"FUGA_RESUELTA_CONFIRMACION#{id_estacion}"
                    
                    elif tipo in ["ORDEN_OoO_RECIBIDO", "ORDEN_ACTIVACION_RECIBIDA"]: 
                        print(f"Estacion available: {id_estacion}")
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
        BD.modificar_estados(id_estacionA, "Disconnected")
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

#def main():
#    print("WM_Central iniciado")


#if __name__ == "__main__":
#    main()

#esto de abajo es el main
def central(Puerto, IP, Puerto_Broker):
    '''Esto seguramente lo divida en dos funciones diferentes, pero
    es para que se entienda el concepto.'''
    #cargarymostrarDatosBD() 
    BD.inicializar()
    estaciones = BD.devolver_estaciones()
    for estacion in estaciones:
        print(f"Estacion : {estacion[0]}, ubicacion: {estacion[1]}, estado: {estacion[2]} ")
    
    '''
        Primero conectara con la BD y cargará los datos.
        Como hasta que una estacion no establezca conexión real
        con la central, su estado debe permanecer en desconectado,
        podemos hacer dos cosas:
            1. Que al cargar se modifiquen todos los estados a desconectado.
            2. Que al apagar el código, las WS se pongan es desconectado para que cuando
            se conecte otra vez la central, salga en desconectado.

            Por tema de errores inesperados que fuercen el apagado de la central, imagino que la opción 2
            no se puede hacer o es más compleja.
    '''
    '''Arrancar el Hilo de Sockets'''
    #crearHiloSockets(Puerto)
    hilo_sockets = threading.Thread(target=servidor_sockets, args=(Puerto,))
    hilo_sockets.daemon = True
    hilo_sockets.start()
    '''Arrancar Hilo Kafka'''
    #crearHiloKafka(IP, Puerto_Broker)
    '''Iniciar Panel de Control'''
    #iniciarPanelControl()
    #quitar los comentarios estos de aqui para probarlo
    #es provisional pq en la practica dice que hay que pasar
    #toda esta info con kafka
    try:
        while True:
            import time
            time.sleep(1)
    except KeyboardInterrupt:
       print("Adios")

if __name__ == "__main__":
   central(5050, "127.0.0.1", 9092)