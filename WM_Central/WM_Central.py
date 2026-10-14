'''Cada paso que haga la central, deberá notificarlo al Operario.'''
import socket
import threading
from Common.config import load_central_config

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
    print(f"[NUEVA CONEXIÓN] {addr} conectado.", flush=True)

    try:
        with conn:
            while True:
                cabecera = recibir_exacto(conn, HEADER)

                if cabecera is None:
                    print(f"[DESCONECTADO] {addr}", flush=True)
                    break

                try:
                    msg_length = int(cabecera.decode(FORMAT).strip())
                except ValueError:
                    print(f"[ERROR] Cabecera inválida de {addr}", flush=True)
                    break

                if msg_length <= 0:
                    print(f"[ERROR] Longitud inválida de {addr}", flush=True)
                    break

                datos = recibir_exacto(conn, msg_length)

                if datos is None:
                    print(f"[ERROR] Mensaje incompleto de {addr}", flush=True)
                    break

                msg = datos.decode(FORMAT)
                print(f"[MENSAJE] {addr}: {msg}", flush=True)

                partes = msg.split("#")

                if len(partes) == 3:
                    registro, id_estacion, ubicacion = partes

                    if registro == "REGISTRO":
                        print(
                            f"Estación: {id_estacion} en {ubicacion}",
                            flush=True,
                        )
                        respuesta = (
                            "STATUS#OK#Estación registrada correctamente"
                        )
                    else:
                        respuesta = (
                            "STATUS#ERROR#Tipo de petición no reconocido"
                        )
                else:
                    respuesta = "STATUS#ERROR#Formato incorrecto"

                respuesta_bytes = respuesta.encode(FORMAT)
                cabecera_respuesta = str(len(respuesta_bytes)).encode(FORMAT).ljust(HEADER, b" ")
                conn.sendall(cabecera_respuesta + respuesta_bytes)

    except (ConnectionError, OSError) as exc:
        print(f"[ERROR DE CONEXIÓN] {addr}: {exc}", flush=True)

    print(f"[CONEXIÓN CERRADA] {addr}", flush=True)

def servidor_sockets(puerto):
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

def main():
    config = load_central_config()

    print("[CENTRAL] WM_Central iniciado", flush=True)
    print(
        f"[CENTRAL] Kafka: {config.kafka_host}:{config.kafka_port}",
        flush=True,
    )
    print(
        f"[CENTRAL] Base de datos: {config.database_path}",
        flush=True,
    )

    servidor_sockets(config.socket_port)

if __name__ == "__main__":
    main()

# def regar():
#     '''Mientras se esté regando, la central recibirá constante información,
#             que mostrará en el panel: el caudal y el volumen acumulado en cada WS'''
#     #if timeout || mensajeFinalirRiego: finalizarRiego()
#     '''Esto lo notificará la WS'''
#     #elif fuga: finalizarRiego() informarModulos() 

# def finalizarRiego():
#     '''Cuando la WS notifique a la central el final del riego, hará lo siguiente: '''
#     #enviarResumenOperario()

# def procesar_peticion_operario():
#     #if petición_de_registro: registrarWS()
#     #elif petición_de_riego: comprobarEstadoWS() 
#         #if estado == correcto: regar()
#         #else: denegarRiego()
#     ''''''

# def procesarPeticionPanel():
#     ''''''
#         #if iniciarRiego: regar()
#         #elif bloquear: bloquearWS()
#         #elif activar: activarWS()

# def handle_client(conn, addr):
#     print(f"[NUEVA CONEXION] {addr} connected.")

#     connected = True
#     while connected:#infinito, cambiar para la practica
#         try: 
#             msg_length = conn.recv(HEADER).decode(FORMAT)
#             if msg_length:
#                 msg_length = int(msg_length)
#                 msg = conn.recv(msg_length).decode(FORMAT)
#                 print(f" He recibido del cliente [{addr}] el mensaje: {msg}")

#                 partes = msg.split('#')
#                 if len(partes) == 3:
#                     registro = partes[0]
#                     id_estacion = partes[1]
#                     ubicacion = partes[2]
#                     if registro == "REGISTRO":
#                         print(f"Estacion: {id_estacion} en {ubicacion}")
#                         respuesta = "STATUS#OK#Estacion registrada correctamente"
#                     else:
#                         respuesta = "STATUS#ERROR#Estacion no registrada correctamente"
                    
#                 else:
#                     respuesta = "Error formato"
                    
#                 conn.send(respuesta.encode(FORMAT))
#             else:
#                 print(f"Cliente ha cerrado la conexion {addr}")
#                 connected = False
#         except:
#             print(f"Conexxion perdida {addr}")
#             connected = False
#     print("ADIOS. TE ESPERO EN OTRA OCASION")
#     conn.close()

# def servidor_sockets(puerto):
#     SERVER = socket.gethostbyname(socket.gethostbyname())
#     ADDR = (SERVER, puerto)

#     servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     servidor.bind(ADDR)
#     servidor.listen()
#     print(f"Servidor a la espera de estaciones en {SERVER}:{puerto}")

#     while True:
#         conn, addr = servidor.accept()
#         thread = threading.Thread(target=handle_client, args=(conn, addr))
#         thread.start()

# def main():
#     print("WM_Central iniciado")

# if __name__ == "__main__":
#     main()

# #esto de abajo es el main
# def central(Puerto, IP, Puerto_Broker):
#     '''Esto seguramente lo divida en dos funciones diferentes, pero
#     es para que se entienda el concepto.'''
#     #cargarymostrarDatosBD() 
#     '''
#         Primero conectara con la BD y cargará los datos.
#         Como hasta que una estacion no establezca conexión real
#         con la central, su estado debe permanecer en desconectado,
#         podemos hacer dos cosas:
#             1. Que al cargar se modifiquen todos los estados a desconectado.
#             2. Que al apagar el código, las WS se pongan es desconectado para que cuando
#             se conecte otra vez la central, salga en desconectado.

#             Por tema de errores inesperados que fuercen el apagado de la central, imagino que la opción 2
#             no se puede hacer o es más compleja.
#     '''
#     '''Arrancar el Hilo de Sockets'''
#     #crearHiloSockets(Puerto)
#     '''Arrancar Hilo Kafka'''
#     #crearHiloKafka(IP, Puerto_Broker)
#     '''Iniciar Panel de Control'''
#     #iniciarPanelControl()
#     #quitar los comentarios estos de aqui para probarlo
#     #es provisional pq en la practica dice que hay que pasar
#     #toda esta info con kafka
#     #try:
#      #   while True:
#       #      pass
#     #except KeyboardInterrupt:
#      #   print("Adios")

#     #if __name__ == "__main__":
#      #   central(5050, "127.0.0.1", 9092)