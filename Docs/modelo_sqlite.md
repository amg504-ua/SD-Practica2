# Modelo de datos SQLite

## Definición del modelo

El sistema utilizará SQLite como mecanismo de persistencia de la información
necesaria para el funcionamiento de `WM_Central`.

El modelo de datos estará formado inicialmente por dos entidades principales:

- `OPERARIOS`
- `WATERING_STATIONS`

### OPERARIOS

| Campo | Descripción |
|---|---|
| `id_operario` | Identificador único del operario |
| `nombre_usuario` | Nombre de usuario del operario |
| `contraseña` | Contraseña del operario |

### WATERING_STATIONS

| Campo | Descripción |
|---|---|
| `id_estacion` | Identificador único de la estación |
| `ubicacion` | Ubicación de la estación |
| `estado` | Estado actual de la estación |

## Relaciones

En el modelo actual no se establece una relación persistente entre
`OPERARIOS` y `WATERING_STATIONS`.

La información correspondiente a los servicios de riego se gestionará
temporalmente en `WM_Central` mientras el servicio se encuentre en ejecución,
sin mantener por el momento un historial persistente de los riegos.

## Persistencia del estado de las estaciones

`WM_Central` utilizará la información almacenada en `WATERING_STATIONS`
para recuperar las estaciones registradas cuando se inicie o reinicie
el sistema.

Si una estación estaba registrada en la base de datos pero todavía no se
ha conectado con `WM_Central`, se mostrará como `DESCONECTADA` hasta que
vuelva a establecer la conexión.