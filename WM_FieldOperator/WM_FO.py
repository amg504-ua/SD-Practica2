import argparse
from Common.config import load_field_operator_config


def menu():
    """Muestra las opciones disponibles para el operario"""
    print("\n=== WM_FO ===")

#def crear_productor_kafka():
#def crear_consumidor_kafka():
#def procesar_evento():
#def leer_ficehro():
#def cargar_interfaz_grafica():

def main(id_operador, ip, puerto_kafka):
    config = load_field_operator_config()

    # Si no se indican argumentos, usamos los valores del .env.
    ip = ip or config.kafka_host
    puerto_kafka = puerto_kafka or config.kafka_port

    print(f"Operario {id_operador} iniciado")
    print(f"Broker Kafka: {ip}:{puerto_kafka}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WM_FieldOperator")

    parser.add_argument("id_operador",help="Identificador del operario")
    #el -- hace que no sea un argumento obligatorio
    parser.add_argument("--ip",help="Dirección IP del broker kafka")
    parser.add_argument("--puerto_kafka",type=int ,help="Puerto del broker kafka")

    args = parser.parse_args()

    main(args.id_operador, args.ip, args.puerto_kafka)