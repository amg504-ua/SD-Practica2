# Topics de Kafka

### Definición de los Topics

| Topic | Productor | Consumidor | Finalidad |
|---|---|---|---|
| `fo.peticion_riego.ce` | `WM_FO` | `WM_Central` | `Enviar solicitudes de riego provenientes de la aplicación del operario` |
| `ce.eventos.fo` | `WM_Central` | `WM_FO` |`Comunicar al operario la autorización / denegación, evolución y resultado del riego` |
| `ce.ordenes.e` | `WM_Central` | `WM_WS_E` |`Envía orden de riego` |
| `e.eventos.ce` | `WM_WS_E` | `WM_Central` | `Comunicar estado del riego` |
---

### Definición de los Mensajes

| Topic | Mensaje |
|---|---|
| `fo.peticion_riego.ce` | `SOLICITUD_RIEGO#ID_OPERARIO#ID_ESTACION#TIEMPO_RIEGO` |
| `ce.ordenes.e` | `ORDEN_RIEGO#ID_OPERARIO#ID_ESTACION#TIEMPO_RIEGO` |
| `e.eventos.ce` | `DATOS_RIEGO#ID_OPERARIO#ID_ESTACION#CAUDAL#VOLUMEN_ACUMULADO` |
| `e.eventos.ce` | `RESUMEN_RIEGO#ID_OPERARIO#ID_ESTACION#TIEMPO_RIEGO#LITROS_SUMINISTRADOS` |
| `ce.eventos.fo` | `RIEGO_AUTORIZADO#ID_OPERARIO#ID_ESTACION` |
| `ce.eventos.fo` | `RIEGO_DENEGADO#ID_OPERARIO#ID_ESTACION` |
| `ce.eventos.fo` | `DATOS_RIEGO#ID_OPERARIO#ID_ESTACION#CAUDAL#VOLUMEN_ACUMULADO` |
| `ce.eventos.fo` | `RESUMEN_RIEGO#ID_OPERARIO#ID_ESTACION#TIEMPO_RIEGO#LITROS_SUMINISTRADOS` |
