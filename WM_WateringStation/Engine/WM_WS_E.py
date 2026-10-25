import socket
import sys
import time
import threading

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

def servidor_monitor(puerto):
    SERVER = socket.gethostbyname(socket.gethostname())
    ADDR = (SERVER, puerto)
    #se pone en 0.0.0.0 para escuchar en las interfaces de la red
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #evita el error de que ya esta en uso.
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        servidor.bind(("0.0.0.0", puerto))
        servidor.listen()

        while True:
            conn, addr = servidor.accept()

            hilo = threading.Thread(
                target=escuchar_monitor,
                args=(conn, addr),
                daemon=True,
            )
            hilo.start()

    finally:
        servidor.close()



#funcion para simular las fugas del monitor
def menu():
    global estado
    while True:
        #se hace un hilo independinete para no bloquear lo demas
        orden = input("Pulsa F para fuga o R para reparar")
        if orden == 'F':
            estado = "KO"
            print("SE ha puesto en fuga, se le comunicara al monitor")
        elif orden == 'R':
            estado = "OK"
            print("Fuga reparada se le comunicara al monitor")
        else:
            print("Tecla equivocada")
    




def main():
    print("WM_WateringStation Engine iniciado")

    if len(sys.argv) >= 2:
        puerto = int(sys.argv[1])
    else:
        puerto =  6000

    #arrancar el servidor en hilo aparte
    hilo_servidor = threading.Thread(
        target=servidor_monitor,
        args=(puerto, ),
        daemon=True
    )
    hilo_servidor.start()

    hilo_fuga = threading.Thread(
        target=menu,
        daemon=True
    )
    hilo_fuga.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Apagando ENgine")

if __name__ == "__main__":
    main()