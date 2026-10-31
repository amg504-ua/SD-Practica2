import sqlite3
from pathlib import Path


def obtener_ruta_bd(id_estacion):
    raiz_proyecto = Path(__file__).resolve().parents[2]

    directorio = raiz_proyecto / "data"
    directorio.mkdir(parents=True, exist_ok=True)

    return directorio / f"engine_{id_estacion}.sqlite"


def obtener_conexion(id_estacion):
    ruta = obtener_ruta_bd(id_estacion)
    return sqlite3.connect(ruta, timeout=10)


def inicializar(id_estacion):
    conexion = obtener_conexion(id_estacion)

    conexion.execute("""
        CREATE TABLE IF NOT EXISTS RIEGOS (
            id_peticion TEXT PRIMARY KEY,
            id_operario TEXT NOT NULL,
            id_estacion TEXT NOT NULL,
            tiempo_objetivo INTEGER NOT NULL,
            tiempo_transcurrido INTEGER DEFAULT 0,
            volumen_acumulado REAL DEFAULT 0,
            estado TEXT NOT NULL
        )
    """)

    conexion.commit()
    conexion.close()

def guardar_riego(id_peticion, id_operario, id_estacion, tiempo_objetivo):
    conexion = obtener_conexion(id_estacion)

    try:
        conexion.execute("""
            INSERT OR IGNORE INTO RIEGOS (
                id_peticion,
                id_operario,
                id_estacion,
                tiempo_objetivo,
                tiempo_transcurrido,
                volumen_acumulado,
                estado
            )
            VALUES (?, ?, ?, ?, 0, 0, 'EN_CURSO')
        """ , (
            id_peticion,
            id_operario,
            id_estacion,
            tiempo_objetivo))

        conexion.commit()

    finally:
        conexion.close()


def actualizar_progreso(
    id_peticion,
    id_estacion,
    tiempo_transcurrido,
    volumen_acumulado
):
    conexion = obtener_conexion(id_estacion)

    try:
        conexion.execute("""
            UPDATE RIEGOS
            SET tiempo_transcurrido = ?,
                volumen_acumulado = ?
            WHERE id_peticion = ?
        """, (
            tiempo_transcurrido,
            volumen_acumulado,
            id_peticion
        ))

        conexion.commit()

    finally:
        conexion.close()


def finalizar_riego(id_peticion, id_estacion):
    conexion = obtener_conexion(id_estacion)

    try:
        conexion.execute("""
            UPDATE RIEGOS
            SET estado = 'FINALIZADO'
            WHERE id_peticion = ?
        """, (id_peticion,))

        conexion.commit()

    finally:
        conexion.close()


def obtener_riego(id_peticion, id_estacion):
    conexion = obtener_conexion(id_estacion)

    try:
        cursor = conexion.execute("""
            SELECT *
            FROM RIEGOS
            WHERE id_peticion = ?
        """, (id_peticion,))

        return cursor.fetchone()

    finally:
        conexion.close()

def obtener_riegos_pendientes(id_estacion):
    conexion = obtener_conexion(id_estacion)
    try:
        cursor = conexion.execute("""
            SELECT *
            FROM RIEGOS
            WHERE id_estacion = ?
            AND estado = 'EN_CURSO'
        """, (id_estacion,))

        return cursor.fetchall()
    finally:
        conexion.close()