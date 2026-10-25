# Topics de Kafka

### Definición de los Topics

| Topic | Productor | Consumidor | Finalidad |
|---|---|---|---|
| `ce.estados.fo` | `WM_Central` | `WM_FO` | `Enviar estados de todas las WS a los operarios` |
| `fo.peticion_riego.ce` | `WM_FO` | `WM_Central` | `Enviar solicitudes de riego provenientes de la aplicación del operario` |
| `ce.eventos.fo` | `WM_Central` | `WM_FO` |`Comunicar al operario la autorización / denegación, evolución y resultado del riego` |
| `ce.ordenes.e` | `WM_Central` | `WM_WS_E` |`Envía orden de riego` |
| `e.eventos.ce` | `WM_WS_E` | `WM_Central` | `Comunicar estado del riego` |
---

### Definición de los Mensajes

| Topic | Mensaje |
|---|---|
| `fo.peticion_riego.ce` | `ID_PETICION#SOLICITUD_RIEGO#ID_OPERARIO#ID_ESTACION#TIEMPO_RIEGO` |
| `ce.ordenes.e` | `ID_PETICION#ORDEN_RIEGO#ID_OPERARIO#ID_ESTACION#TIEMPO_RIEGO` |
| `e.eventos.ce` | `ID_PETICION#DATOS_RIEGO#ID_OPERARIO#ID_ESTACION#CAUDAL#VOLUMEN_ACUMULADO` |
| `e.eventos.ce` | `ID_PETICION#RESUMEN_RIEGO#ID_OPERARIO#ID_ESTACION#TIEMPO_RIEGO#LITROS_SUMINISTRADOS` |
| `ce.eventos.fo` | `ID_PETICION#RIEGO_AUTORIZADO#ID_OPERARIO#ID_ESTACION` |
| `ce.eventos.fo` | `ID_PETICION#RIEGO_DENEGADO#ID_OPERARIO#ID_ESTACION#MOTIVO` |
| `ce.eventos.fo` | `ID_PETICION#DATOS_RIEGO#ID_OPERARIO#ID_ESTACION#CAUDAL#VOLUMEN_ACUMULADO` |
| `ce.eventos.fo` | `ID_PETICION#RESUMEN_RIEGO#ID_OPERARIO#ID_ESTACION#TIEMPO_RIEGO#LITROS_SUMINISTRADOS` |
| `ce.eventos.fo` | `ID_PETICION#RIEGO_INTERRUMPIDO#ID_OPERARIO#ID_ESTACION#MOTIVO` |
| `ce.estados.fo` | `ID_ESTACION#ESTADO#UBICACION#CAUDAL#VOLUMEN_ACUMULADO#ID_OPERARIO` |
