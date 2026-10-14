"""Lee las variables de entorno, comprueba sus valores y permite que
Python las utilice."""

import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

# Ruta del archivo .env situado en la raíz del proyecto
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = PROJECT_ROOT / ".env"

# Cargar las variables locales sin sobrescribir las ya existentes
load_dotenv(dotenv_path=ENV_PATH, override=False)

def _required_str(name: str) -> str:
    """Obtiene una variable de entorno obligatoria."""
    value = os.getenv(name)

    if value is None or not value.strip():
        raise ValueError(
            f"Falta la variable de entorno obligatoria: {name}"
        )

    return value.strip()

def _required_int(name: str) -> int:
    """Obtiene una variable de entorno entera y valida su formato."""
    value = _required_str(name)

    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(
            f"La variable {name} debe ser un entero."
        ) from exc

def _port(name: str) -> int:
    """Obtiene un puerto TCP válido."""
    port = _required_int(name)

    if not 1 <= port <= 65535:
        raise ValueError(
            f"La variable {name} debe estar entre 1 y 65535."
        )

    return port

@dataclass(frozen=True) #Una vez creado el objeto, no se pueden reasignar sus atributos
class CentralConfig:
    socket_port: int
    kafka_host: str
    kafka_port: int
    database_path: str

@dataclass(frozen=True)
class FieldOperatorConfig:
    kafka_host: str
    kafka_port: int

@dataclass(frozen=True)
class MonitorConfig:
    socket_port: int
    central_host: str
    central_port: int

@dataclass(frozen=True)
class EngineConfig:
    monitor_host: str
    monitor_port: int
    kafka_host: str
    kafka_port: int

def load_central_config() -> CentralConfig:
    return CentralConfig(
        socket_port=_port("CENTRAL_SOCKET_PORT"),
        kafka_host=_required_str("KAFKA_HOST"),
        kafka_port=_port("KAFKA_PORT"),
        database_path=_required_str("DATABASE_PATH"),
    )

def load_field_operator_config() -> FieldOperatorConfig:
    return FieldOperatorConfig(
        kafka_host=_required_str("KAFKA_HOST"),
        kafka_port=_port("KAFKA_PORT"),
    )

def load_monitor_config() -> MonitorConfig:
    return MonitorConfig(
        socket_port=_port("MONITOR_SOCKET_PORT"),
        central_host=_required_str("CENTRAL_HOST"),
        central_port=_port("CENTRAL_PORT"),
    )

def load_engine_config() -> EngineConfig:
    return EngineConfig(
        monitor_host=_required_str("MONITOR_HOST"),
        monitor_port=_port("MONITOR_PORT"),
        kafka_host=_required_str("KAFKA_HOST"),
        kafka_port=_port("KAFKA_PORT"),
    )
