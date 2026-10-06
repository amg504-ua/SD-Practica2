# Topics de Kafka

#### Definición de los Topics

| Topic | Productor | Consumidor | Finalidad |
|---|---|---|---|
| `fo.peticion_riego.ce` | `WM_FO` | `WM_Central` | `Enviar solicitudes de riego provinientes de la aplicación del operario` |
| `ce.eventos.fo` | `WM_Central` | `WM_FO` |`Comunicar al operario la autorización / denegación, evolución y resultado del riego` |
| `ce.ordenes.e` | `WM_Central` | `WM_WS_E` |`Envía orden de riego` |
| `e.eventos.ce` | `WM_WS_E` | `WM_Central` | `Comunicar estado del riego` |
---

| Topic | Mensaje |
|---|---|
| `fo.peticion_riego.ce` | `SOLICITUD_RIEGO#ID_Operario#ID_Estacion#TIEMPO_RIEGO` |

ce.eventos.fo
- riego aceptado
- riego denegado
- notificación al operario de los pasos hasta autorizar o denegar el riego

e.resumen.ce
- caudal
- volumen acumulado
- envia resumen riego

ce.e
- envia solicitud riego

| `WM_Central` | `WM_WS_E` | ORDEN_RIEGO#ID_ESTACION#TIEMPO_RIEGO\n | ORDEN_RIEGO_RECIBIDO#ID_ESTACION\n |

| `WM_WS_E` | `WM_Central` | RESUMEN_RIEGO#ID_ESTACION#TIEMPO_RIEGO#LITROS_SUMINISTRADOS\n | RESUMEN_RIEGO_RECIBIDO#ID_ESTACION\n |
