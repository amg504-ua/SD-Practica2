import socket

def enviar_orden():
    cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        cliente.connect(('127.0.0.1', 5050))
        while True:
            print("\n------ MENU DE LA CENTRAL ------")
            print("1. Iniciar un Riego")
            print("2. Bloquear la WS(Fuera de Servicio)")
            print("3. Activar la WS")
            print("4. Salir")
            
            opcion = input("Introduce tu orden: ")

            if opcion == '1':
                id_estacion = input("Introduce el id de la estacion a regar: ")
                tiempo = input("Introduce la duracion del riego en segundos: ")
                mensaje = f"CENTRAL_RIEGA#{id_estacion}#{tiempo}\n"
            elif opcion == '2':
                id_estacion = input("Introduce el id de la estacion a bloquear: ")
                mensaje = f"CENTRAL_BLOQUEA#{id_estacion}\n"
            elif opcion == '3':
                id_estacion = input("Introduce el id de la estacion a activar: ")
                mensaje = f"CENTRAL_ACTIVA#{id_estacion}\n"
            elif opcion == '4':
                print("Cerrando menu de central")
                break
            else:
                print("opcion no valida")
                continue

            cliente.send(mensaje.encode('utf-8'))
            print(f"respuesta de la central: {cliente.recv(1024).decode('utf-8').strip()}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        cliente.close()

if __name__ == "__main__":
    enviar_orden()



    