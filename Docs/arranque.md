# Guía de arranque — WaterManagement

## 1. Requisitos previos

- Python 3 instalado.
- Repositorio clonado localmente.
- Dependencia `python-dotenv` instalada.
- Archivo `.env` configurado a partir de `.env.example`.

No se utiliza un entorno virtual de Python.

## 2. Preparación inicial

Todos los comandos se ejecutan desde la raíz del repositorio.

### 2.1. Instalar las dependencias

```bash
python3 -m pip install --user -r requirements.txt
```

Este comando instala las dependencias declaradas en `requirements.txt` para el usuario actual.

Si la distribución de Linux bloquea esta instalación, consulta las instrucciones de instalación de dependencias del proyecto antes de modificar el Python del sistema.

### 2.2. Crear la configuración local

La primera vez que se prepara el proyecto:

```bash
cp .env.example .env
```

Este comando crea una copia de la plantilla. Si `.env` ya existe, no debe ejecutarse de nuevo sin conservar previamente su configuración.

El archivo `.env` contiene los valores utilizados en el entorno local y no debe subirse a Git.

### 2.3. Revisar la configuración

Comprobar que `.env` contiene las variables necesarias:

- `KAFKA_HOST`
- `KAFKA_PORT`
- `CENTRAL_SOCKET_PORT`
- `DATABASE_PATH`
- `MONITOR_SOCKET_PORT`
- `CENTRAL_HOST`
- `CENTRAL_PORT`
- `MONITOR_HOST`
- `MONITOR_PORT`

Los identificadores del operador y de la estación se proporcionarán mediante argumentos de arranque, una vez implementado su análisis.

## 3. Puntos de entrada

Los comandos se ejecutan desde la raíz del repositorio.

### 3.1. WM_Central

```bash
python3 -m WM_Central.WM_Central
```

Este componente utilizará la configuración del servidor de sockets, Kafka y SQLite.

### 3.2. WM_FieldOperator

```bash
python3 -m WM_FieldOperator.WM_FO
```

Este componente utilizará el identificador del operador y la configuración de Kafka.

El argumento del identificador se incorporará al implementar el análisis de argumentos de línea de comandos.

### 3.3. WM_WateringStation — Monitor

```bash
python3 -m WM_WateringStation.Monitor.WM_WS_M
```

Este componente utilizará el identificador de la estación, su puerto de escucha y la dirección de la Central.

El argumento del identificador se incorporará al implementar el análisis de argumentos de línea de comandos.

### 3.4. WM_WateringStation — Engine

```bash
python3 -m WM_WateringStation.Engine.WM_WS_E
```

Este componente utilizará la dirección del Monitor y la configuración de Kafka. El identificador de la estación se recibirá mediante sockets.

## 4. Configuración del proyecto

### `.env.example`

Plantilla versionada que documenta las variables de entorno necesarias y proporciona valores de ejemplo.

### `.env`

Archivo de configuración local. Se excluye del repositorio mediante `.gitignore`.

### `Common/config.py`

Carga las variables de entorno mediante `python-dotenv`, valida los valores y proporciona objetos de configuración específicos para cada módulo.

### `requirements.txt`

Contiene las dependencias Python del proyecto.

## 5. Estado actual del desarrollo

La fase de esqueleto prepara la estructura, la configuración y los puntos de entrada.

La ejecución completa de los componentes requerirá implementar progresivamente:

- Comunicación mediante sockets.
- Comunicación mediante Kafka.
- Inicialización y operaciones con SQLite.
- Análisis de argumentos de arranque.
- Lógica funcional de cada módulo.

Los comandos anteriores describen los puntos de entrada previstos. Se comprobará su ejecución cuando los archivos correspondientes estén preparados para arrancar.

## 6. Buenas prácticas

- Ejecutar los comandos desde la raíz del repositorio.
- No subir `.env` a Git.
- Mantener `.env.example` actualizado cuando cambien las variables de configuración.
- Actualizar `requirements.txt` cuando se incorporen dependencias.
- Documentar los cambios relevantes en `Docs/`.
