'''Cada paso que haga la central, deberá notificarlo al Operario.'''

import threading

def regar():
    '''Mientras se esté regando, la central recibirá constante información,
            que mostrará en el panel: el caudal y el volumen acumulado en cada WS'''
    #if timeout || mensajeFinalirRiego: finalizarRiego()
    '''Esto lo notificará la WS'''
    #elif fuga: finalizarRiego() informarModulos() 

def finalizarRiego():
    '''Cuando la WS notifique a la central el final del riego, hará lo siguiente: '''
    #enviarResumenOperario()

def procesar_peticion_operario():
    #if petición_de_registro: registrarWS()
    #elif petición_de_riego: comprobarEstadoWS() 
        #if estado == correcto: regar()
        #else: denegarRiego()
    ''''''

def procesarPeticionPanel():
    ''''''
        #if iniciarRiego: regar()
        #elif bloquear: bloquearWS()
        #elif activar: activarWS()

def central(Puerto, IP, Puerto_Broker):
    '''Esto seguramente lo divida en dos funciones diferentes, pero
    es para que se entienda el concepto.'''
    #cargarymostrarDatosBD() 
    '''
        Primero conectara con la BD y cargará los datos.
        Como hasta que una estacion no establezca conexión real
        con la central, su estado debe permanecer en desconectado,
        podemos hacer dos cosas:
            1. Que al cargar se modifiquen todos los estados a desconectado.
            2. Que al apagar el código, las WS se pongan es desconectado para que cuando
            se conecte otra vez la central, salga en desconectado.

            Por tema de errores inesperados que fuercen el apagado de la central, imagino que la opción 2
            no se puede hacer o es más compleja.
    '''
    '''Arrancar el Hilo de Sockets'''
    #crearHiloSockets(Puerto)
    '''Arrancar Hilo Kafka'''
    #crearHiloKafka(IP, Puerto_Broker)
    '''Iniciar Panel de Control'''
    #iniciarPanelControl()