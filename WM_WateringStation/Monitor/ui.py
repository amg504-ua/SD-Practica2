
import tkinter as tk
from queue import Queue, Empty


class MonitorGUI:

    COLORES = {
        "DISPONIBLE": "#16834A",
        "REGANDO": "#16834A",
        "AVERIADO": "#DC2626",
        "OUT_OF_ORDER": "#EA8A16",
        "DESCONECTADO": "#64748B",
        "CONECTANDO": "#475569"
    }

    ETIQUETAS = {
        "DISPONIBLE": "DISPONIBLE",
        "REGANDO": "REGANDO",
        "AVERIADO": "AVERIADO",
        "OUT_OF_ORDER": "OUT OF ORDER",
        "DESCONECTADO": "DESCONECTADO",
        "CONECTANDO": "CONECTANDO..."
    }

    def __init__(self, id_estacion, ubicacion):

        self.root = tk.Tk()
        self.root.title(f"Water Management - {id_estacion}")
        self.root.geometry("540x430")
        self.root.resizable(False, False)
        self.root.configure(bg="#17212B")

        self.id_estacion = id_estacion
        self.ubicacion = ubicacion

        self.eventos = Queue()
        self.estado = "CONECTANDO"
        self.parpadeo = False

        self.crear_interfaz()

        self.root.after(100, self.procesar_eventos)
        self.root.after(500, self.actualizar_parpadeo)

    def crear_interfaz(self):

        # Cabecera
        cabecera = tk.Frame(
            self.root,
            bg="#17212B",
            height=65
        )
        cabecera.pack(fill="x")

        tk.Label(
            cabecera,
            text="WATER MANAGEMENT",
            font=("Arial", 15, "bold"),
            fg="white",
            bg="#17212B"
        ).pack(side="left", padx=20, pady=18)

        tk.Label(
            cabecera,
            text="MONITOR",
            font=("Arial", 10, "bold"),
            fg="#60A5FA",
            bg="#17212B"
        ).pack(side="right", padx=20)

        # Panel principal
        self.panel = tk.Frame(
            self.root,
            bg=self.COLORES["CONECTANDO"]
        )
        self.panel.pack(fill="both", expand=True)

        self.etiqueta_id = tk.Label(
            self.panel,
            text=self.id_estacion,
            font=("Arial", 26, "bold"),
            fg="white",
            bg=self.COLORES["CONECTANDO"]
        )
        self.etiqueta_id.pack(pady=(35, 5))

        self.etiqueta_ubicacion = tk.Label(
            self.panel,
            text=self.ubicacion,
            font=("Arial", 12),
            fg="white",
            bg=self.COLORES["CONECTANDO"]
        )
        self.etiqueta_ubicacion.pack()

        self.etiqueta_estado = tk.Label(
            self.panel,
            text="CONECTANDO...",
            font=("Arial", 23, "bold"),
            fg="white",
            bg=self.COLORES["CONECTANDO"]
        )
        self.etiqueta_estado.pack(pady=30)

        self.etiqueta_componentes = tk.Label(
            self.panel,
            text="Monitor: INICIANDO | Engine: INICIANDO",
            font=("Arial", 10),
            fg="white",
            bg=self.COLORES["CONECTANDO"]
        )
        self.etiqueta_componentes.pack()

        # Panel de mensajes
        panel_mensajes = tk.Frame(
            self.root,
            bg="#17212B",
            height=105
        )
        panel_mensajes.pack(fill="x", side="bottom")

        tk.Label(
            panel_mensajes,
            text="ÚLTIMO MENSAJE DEL MONITOR",
            font=("Arial", 9, "bold"),
            fg="#60A5FA",
            bg="#17212B"
        ).pack(anchor="w", padx=15, pady=(10, 5))

        self.ultimo_mensaje = tk.Label(
            panel_mensajes,
            text="Esperando actividad...",
            font=("Consolas", 9),
            fg="#86EFAC",
            bg="#17212B",
            wraplength=500,
            justify="left"
        )
        self.ultimo_mensaje.pack(
            anchor="w",
            padx=15,
            pady=(0, 10)
        )

    def publicar_estado(self, estado):
        self.eventos.put(("ESTADO", estado))

    def publicar_mensaje(self, mensaje):
        self.eventos.put(("MENSAJE", mensaje))

    def publicar_componentes(self, monitor, engine):
        self.eventos.put(("COMPONENTES", monitor, engine))

    def cambiar_color(self, color):

        self.panel.configure(bg=color)

        for etiqueta in (
            self.etiqueta_id,
            self.etiqueta_ubicacion,
            self.etiqueta_estado,
            self.etiqueta_componentes
        ):
            etiqueta.configure(bg=color)

    def cambiar_estado(self, estado):

        if estado not in self.COLORES:
            return

        self.estado = estado

        self.etiqueta_estado.configure(
            text=self.ETIQUETAS[estado]
        )

        self.cambiar_color(self.COLORES[estado])

    def procesar_eventos(self):

        try:
            while True:

                evento = self.eventos.get_nowait()
                tipo = evento[0]

                if tipo == "ESTADO":
                    self.cambiar_estado(evento[1])

                elif tipo == "MENSAJE":
                    self.ultimo_mensaje.configure(
                        text=evento[1]
                    )

                elif tipo == "COMPONENTES":
                    self.etiqueta_componentes.configure(
                        text=f"Monitor: {evento[1]} | Engine: {evento[2]}"
                    )

        except Empty:
            pass

        self.root.after(100, self.procesar_eventos)

    def actualizar_parpadeo(self):

        if self.estado == "REGANDO":

            self.parpadeo = not self.parpadeo

            color = (
                "#16834A"
                if self.parpadeo
                else "#75D99A"
            )

            self.cambiar_color(color)

        self.root.after(500, self.actualizar_parpadeo)

    def ejecutar(self):
        self.root.mainloop()
