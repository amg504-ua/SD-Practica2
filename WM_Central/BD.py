import sqlite3
import os

#guardar el archivo de la base de datos en la carpeta de cenral
Ruta_BD = os.path.join(os.path.dirname(__file__), "central.db")

#funciones para la base de datos
def obtener_conexion():
    #con esto se conecta a la base de datos, el time out sirve para
    #evitar bloqueos cuando dos estaciones actuen a la vez
    return sqlite3.connect(Ruta_BD, timeout=10)

#iniciaizar la base de datos y crearla si no existe
def inicializar():
    conexion = obtener_conexion()
    creador = conexion.cursor()

    #crear la tabla dde operarios
    #creador.execute('''
    #CREATE TABLE IF NOT EXISTS OPERARIOS (
    #        id_operario TEXT PRIMARY KEY,
    #        nombre_usuario TEXT NOT NULL,
    #        contraseña TEXT NOT NULL
    #    )
    #''')
    #crear la tabla dde estaciones de riego
    creador.execute('''
    CREATE TABLE IF NOT EXISTS WATERING_STATIONS (
            id_estacion TEXT PRIMARY KEY,
            ubicacion TEXT NOT NULL,
            estado TEXT NOT NULL
        )
    ''')
    creador.execute("UPDATE WATERING_STATIONS SET ESTADO = 'DESCONECTADA'")

    conexion.commit()
    conexion.close()
    print("base de datos inicializada, las estaciones ya guardadas se marcan como desconectadas")

#Funcion para guardar las estaciones ne la bd
def guardar_estacion(id_estacion, ubicacion, estado_inicial="DISPONIBLE"):
    conexion = obtener_conexion()
    creador = conexion.cursor()

    #insertar si no existe o actualizar si ya existe
    creador.execute('''
        INSERT OR REPLACE INTO WATERING_STATIONS (id_estacion, ubicacion, estado)
        VALUES (?, ?, ?)
    ''', (id_estacion, ubicacion, estado_inicial))
    
    conexion.commit()
    conexion.close()
    print(f"estacion '{id_estacion}' con ubicacion: {ubicacion} guardada como '{estado_inicial}")

#funcion para modificar los estados de las maquinas
def modificar_estados(id_estacion, nuevo_estado):
    conexion = obtener_conexion()
    creador = conexion.cursor()
    
    #insertar si no existe o actualizar si ya existe
    creador.execute('''
        UPDATE WATERING_STATIONS
        SET estado = ?
        WHERE id_estacion = ?
    ''', (id_estacion, nuevo_estado))

    conexion.commit()
    conexion.close()
    print(f"Estado de la estacion {id_estacion} actualizado a {nuevo_estado}")

#funcion para devolver las estaciones registradas
def devolver_estaciones():
    conexion = obtener_conexion()
    creador = conexion.cursor()
    creador.execute("SELECT id_estacion, ubicacion, estado FROM WATERING_STATIONS")
    estaciones = creador.fetchall()
    conexion.close()
    return estaciones

