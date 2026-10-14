import socket
import sys

HEADER = 64
FORMAT = 'utf-8'

def send(socketC, msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    socketC.send(send_length)
    socketC.send(message)
    
########## MAIN ##########


print("****** WELCOME TO OUR BRILLIANT SD UA CURSO 2020/2021 SOCKET CLIENT ****")

if  (len(sys.argv) == 5):
    SERVER = sys.argv[1]
    PORT = int(sys.argv[2])
    ADDR = (SERVER, PORT)
    ID_Estacion = sys.argv[3]
    Ubicacion = sys.argv[4]

    try:

        #creamos el socket
        cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cliente.connect(ADDR)#establecer conexion
        print (f"Establecida conexión en [{ADDR}]")

        mensaje = f"REGISTRO#{ID_Estacion}#{Ubicacion}"
        print("Envio al servidor: ", mensaje)
        send(cliente, mensaje)#enviar la orden
        respuesta = cliente.recv(2048).decode(FORMAT)#esperar
        print(f"Recibido: {respuesta}")

        cliente.close()#cerrar
    except ConnectionRefusedError:
        print(f"Conexion rexhazada")
else:
    print ("Oops!. Parece que algo falló. Necesito estos argumentos: <ServerIP> <Puerto> <Texto Bienvenida>")
