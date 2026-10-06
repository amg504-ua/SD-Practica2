# Protocolo de Sockets

### Formato de los mensajes

Los mensajes del protocolo siguen el formato:

TIPO#CAMPO1#CAMPO2#...

El primer campo identifica el tipo de mensaje y los campos posteriores
contienen los datos asociados al mensaje.

Los campos son separados mediante el carácter `#`.
El final de cada mensaje se delimita mediante `\n`

Si el WM_WS_E no responde dentro del tiempo establecido a una comprobación de estado, el WM_WS_M considera que se ha producido un TIMEOUT y comunica la incidencia a WM_Central.

#### Definición de los Mensajes de los Sockets

| Origen | Destino | Mensaje | Respuesta |
|---|---|---|---|
| `WM_WS_M` | `WM_Central` | REGISTRO#ID_ESTACION#UBICACION\n | REGISTRO_OK#ID_ESTACION\n / REGISTRO_ERROR#ID_ESTACION\n |
| `WM_WS_M` | `WM_Central` | CONEXION#ID_ESTACION\n | CONEXION_OK#ID_ESTACION\n / CONEXION_ERROR#ID_ESTACION\n |
| `WM_WS_M` | `WM_Central` | AVISO_FUGA#ID_ESTACION\n | AVISO_FUGA_RECIBIDO#ID_ESTACION\n |
| `WM_WS_M` | `WM_Central` | FUGA_RESUELTA#ID_ESTACION\n | FUGA_RESUELTA_CONFIRMACION#ID_ESTACION\n |
| `WM_Central` | `WM_WS_M` | OUT_OF_ORDER#ID_ESTACION\n | ORDEN_OoO_RECIBIDO#ID_ESTACION\n |
| `WM_Central` | `WM_WS_M` | ACTIVACION#ID_ESTACION\n | ORDEN_ACTIVACION_RECIBIDA#ID_ESTACION\n |
| `WM_WS_M` | `WM_WS_E` | CONEXION_ENGINE#ID_ESTACION\n | CONEXION_OK#ID_ESTACION\n / CONEXION_ERROR#ID_ESTACION\n | 
| `WM_WS_M` | `WM_WS_E` | COMPROBACION_ESTADO_WS#ID_ESTACION\n | ESTADO_OK#ID_ESTACION\n / KO#ID_ESTACION\n | 
---


