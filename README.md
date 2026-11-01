# SD-Practica2

1 Requisitos previos y entorno.
Sistema operativo: Linux (Ubuntu/Debian) o compatible.
Software requerido: Docker, Docker Compose y Python 3 instalados.
Situarse en la raíz del proyecto.

2 Configuración del .env
KAFKA_HOST=tokaido.proxy.rlwy.net
KAFKA_PORT=40908
CENTRAL_HOST=acela.proxy.rlwy.net
CENTRAL_PORT=43731

3 Ejecución
Se abre una terminal para cada módulo y ejecuta docker compose build.

Ejemplo Engine: docker compose run --rm --name engine-01 wm-engine python -u WM_WateringStation/Engine/WM_WS_E.py 6001

Ejemplo Monitor: docker compose run --rm --name monitor-01 wm-monitor python -u WM_WateringStation/Monitor/WM_WS_M.py acela.proxy.rlwy.net <PUERTO_CENTRAL> engine-01 6001 WS-01 "Huerto universitario"

Ejemplo operario: docker compose run --rm --name fo-01 wm-fo python -u WM_FieldOperator/WM_FO.py FO-01
