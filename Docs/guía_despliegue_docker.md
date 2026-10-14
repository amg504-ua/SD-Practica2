# Guía de despliegue con Docker — WaterManagement

## 1. Objetivo

Esta guía explica cómo construir las imágenes Docker del proyecto WaterManagement, arrancar sus servicios y comprobar su estado durante el desarrollo local.

El proyecto dispone de cuatro componentes:

- **WM_Central:** servidor central de comunicación.
- **WM_FO:** operador de campo.
- **WM_WS_E:** motor de la estación de riego (*Engine*).
- **WM_WS_M:** monitor de la estación de riego (*Monitor*).

## 2. Requisitos previos

Es necesario disponer de:

- Docker Engine.
- Docker Compose.
- Python, para ejecutar las pruebas locales que lo requieran.
- El código fuente del proyecto y sus archivos de configuración.

La estructura relevante es:

```text
SD-Practica2/
├── Common/
├── WM_Central/
│   └── Dockerfile
├── WM_FieldOperator/
│   └── Dockerfile
├── WM_WateringStation/
│   ├── Engine/
│   │   └── Dockerfile
│   └── Monitor/
│       └── Dockerfile
├── .env
├── .env.example
├── .dockerignore
├── docker-compose.yml
└── requirements.txt
```

## 3. Configuración

Antes de iniciar los servicios, hay que comprobar que existe el archivo `.env` y que contiene las variables necesarias para cada componente.

Docker Compose utiliza este archivo para proporcionar a WM_Central las variables de entorno configuradas mediante `env_file`.

El archivo `.env` no debe incorporarse a las imágenes ni publicarse en el repositorio. Se debe mantener actualizado `.env.example` con los nombres de las variables necesarias, pero sin valores secretos.

## 4. Construcción de las imágenes

Desde la raíz del proyecto, ejecutar:

```bash
sudo docker compose config -q
sudo docker compose build
```

El primer comando comprueba la configuración de Compose. El segundo construye las imágenes definidas en `docker-compose.yml`.

Para comprobar las imágenes creadas:

```bash
sudo docker images
```

Deben aparecer las imágenes correspondientes a los cuatro componentes, con la etiqueta `dev`.

## 5. Arranque de WM_Central

Para iniciar únicamente el servidor central:

```bash
sudo docker compose up -d wm-central
```

Para comprobar su estado:

```bash
sudo docker compose ps
```

Para consultar sus registros:

```bash
sudo docker compose logs wm-central
```

El servidor debe arrancar y escuchar en el puerto 5000. La publicación del puerto en Compose permite acceder a él desde el ordenador anfitrión mediante `127.0.0.1:5000`.

## 6. Comprobación de conectividad

Para verificar que el puerto está accesible desde el anfitrión:

```bash
python -c "import socket; s=socket.create_connection(('127.0.0.1', 5000), timeout=3); print('Conexión TCP correcta'); s.close()"
```

Esta prueba comprueba la conectividad TCP. Para verificar el protocolo de aplicación, debe enviarse un mensaje válido según el protocolo definido por el proyecto y comprobarse la respuesta de Central.

## 7. Detención de los servicios

Para detener los servicios gestionados por Compose:

```bash
sudo docker compose down
```

Este comando detiene y elimina los contenedores y la red creada por Compose. Las imágenes construidas se conservan.

## 8. Diagnóstico básico

| Problema | Comprobación |
|---|---|
| Error de permisos | Ejecutar los comandos Docker con `sudo`. |
| Error al construir una imagen | Revisar el Dockerfile y la salida de `docker compose build`. |
| El contenedor se detiene | Consultar `sudo docker compose logs NOMBRE_SERVICIO`. |
| No se puede conectar al servidor | Comprobar el estado del contenedor y la publicación del puerto. |
| Faltan variables de configuración | Revisar `.env` localmente, sin compartir sus valores. |

## 9. Arquitectura del despliegue final

La ejecución local con Docker Compose se utiliza para desarrollar y probar los componentes.

La arquitectura de despliegue prevista es diferente:

- **Railway:** WM_Central, SQLite y Kafka.
- **Ordenadores del laboratorio:** WM_FO y las estaciones de riego, incluyendo Engine y Monitor.

Por tanto, antes del despliegue definitivo será necesario configurar las direcciones de red, los puertos, las variables de entorno y la persistencia de SQLite según el entorno de destino.

## 10. Estado actual

Las cuatro imágenes Docker se han construido correctamente. La configuración de Compose se valida y WM_Central arranca en un contenedor, publica el puerto 5000 y acepta conexiones TCP desde el anfitrión.

Esto valida la base de Docker, pero no demuestra todavía la integración funcional completa de los cuatro componentes con Kafka y SQLite.
