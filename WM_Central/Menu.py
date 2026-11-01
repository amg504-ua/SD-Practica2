import os
import socket
from BD import devolver_estaciones

# Sin valores fijos en el codigo: se leen del entorno (mismo puerto que usa WM_Central)
HOST = os.getenv("CENTRAL_MENU_HOST", "127.0.0.1")
PUERTO = int(os.getenv("CENTRAL_SOCKET_PORT", "5050"))

# Activa los colores ANSI en la consola de Windows (en Linux/Railway no hace nada)
os.system("")

RESET = "\033[0m"
ANCHO_ESTADO = 36

# codigo ANSI: 30 = texto negro, 97 = texto blanco, 42 = fondo verde, 41 = fondo rojo,
# 48;5;208 = fondo naranja, 48;5;245 = fondo gris, 5 = parpadeo (si el terminal lo soporta)
VERDE = "\033[30;42m"
VERDE_PARPADEANTE = "\033[5;30;42m"
ROJO = "\033[97;41m"
NARANJA = "\033[30;48;5;208m"
GRIS = "\033[30;48;5;245m"


def obtener_estados(estado):
    """Devuelve (texto, color) segun el estado guardado en la BD."""
    e = str(estado).strip().upper().replace("_", " ")

    if e in ("AVAILABLE", "DISPONIBLE"):
        return "Disponible", VERDE
    if e in ("WATERING", "REGANDO"):
        return "Regando", VERDE_PARPADEANTE
    if e in ("LEAK", "FUGA", "AVERIADA", "AVERIADO"):
        return "Fuga", ROJO
    if e in ("OUT OF SERVICE", "FUERA DE SERVICIO"):
        return "Fuera de Servicio (Out of Service)", NARANJA
    if e in ("DISCONNECTED", "DESCONECTADA"):
        return "Desconectada", GRIS
    return f"Desconocido ({estado})", GRIS


def _numero(valor):
    """El caudal/volumen pueden venir a None desde la BD: no debe romper el panel."""
    try:
        return f"{float(valor):.1f}"
    except (TypeError, ValueError):
        return "-"


def mostrar_panel():
    estaciones = devolver_estaciones()

    print("\n" + "=" * 102)
    print(f"{'ID':<7} | {'UBICACION':<18} | {'CAUDAL (L/m)':<12} | {'VOLUMEN (L)':<11} | "
          f"{'OPERARIO':<9} | {'ESTADO'}")
    print("-" * 102)

    if not estaciones:
        print("(no hay estaciones registradas)")

    for fila in estaciones:
        # por si alguna version de la BD devolviera menos columnas
        ws_id, ubicacion, estado, caudal, volumen, operario = (tuple(fila) + (None,) * 6)[:6]
        texto, color = obtener_estados(estado)

        if texto == "Regando":
            caudal_str = _numero(caudal)
            volumen_str = _numero(volumen)
            operario_str = str(operario) if operario else "-"
        else:
            caudal_str = volumen_str = operario_str = "-"

        # el color solo envuelve la ultima columna, asi no se descuadra la tabla
        celda = f"{color}{texto.center(ANCHO_ESTADO)}{RESET}"
        print(f"{str(ws_id):<7} | {str(ubicacion):<18} | {caudal_str:<12} | {volumen_str:<11} | "
              f"{operario_str:<9} | {celda}")
    print("=" * 102)


def ids_destino(texto):
    """Una estacion concreta o TODAS las registradas."""
    if texto.strip().upper() in ("TODAS", "TODOS", "*", "ALL"):
        return [str(f[0]) for f in devolver_estaciones()]
    return [texto.strip()]


def enviar(cliente, mensaje):
    cliente.send((mensaje + "\n").encode("utf-8"))
    respuesta = cliente.recv(1024)
    if not respuesta:
        raise ConnectionError("WM_Central ha cerrado la conexion")
    return respuesta.decode("utf-8").strip()


def menu():
    cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        cliente.connect((HOST, PUERTO))
        while True:
            # El panel se muestra SIEMPRE antes del menu: todo junto en la misma pantalla
            mostrar_panel()
            print("\n------ MENU DE LA CENTRAL ------")
            print("1. Iniciar un Riego")
            print("2. Bloquear la WS (Fuera de Servicio)")
            print("3. Activar la WS")
            print("4. Recargar panel")
            print("5. Salir")
            print("(en 1, 2 y 3 puedes escribir TODAS en lugar de un id)")

            opcion = input("Introduce tu orden: ").strip()

            if opcion == "1":
                destino = input("Introduce el id de la estacion a regar (o TODAS): ")
                tiempo = input("Introduce la duracion del riego en segundos: ")
                plantilla = "CENTRAL_RIEGA#{id}#" + tiempo.strip()
            elif opcion == "2":
                destino = input("Introduce el id de la estacion a bloquear (o TODAS): ")
                plantilla = "CENTRAL_BLOQUEA#{id}"
            elif opcion == "3":
                destino = input("Introduce el id de la estacion a activar (o TODAS): ")
                plantilla = "CENTRAL_ACTIVA#{id}"
            elif opcion == "4":
                continue  # vuelve al principio del bucle, que repinta el panel
            elif opcion == "5":
                print("Cerrando central")
                break
            else:
                print("opcion no valida")
                continue

            ids = ids_destino(destino)
            if not ids:
                print("No hay estaciones registradas.")
                continue

            for id_estacion in ids:
                respuesta = enviar(cliente, plantilla.replace("{id}", id_estacion))
                print(f"[{id_estacion}] respuesta de la central: {respuesta}")

    except (ConnectionError, OSError) as e:
        print(f"Imposible conectar con WM_Central ({HOST}:{PUERTO}): {e}")
    except KeyboardInterrupt:
        print("\nMenu cerrado.")
    finally:
        cliente.close()


if __name__ == "__main__":
    menu()