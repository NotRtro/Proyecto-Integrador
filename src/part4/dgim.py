"""DGIM: cuántos 1 hubo en la ventana reciente, con memoria O(log² N).

Motor: Python puro (estructura de flujo; procesa un evento a la vez).

Cada bucket guarda (tamaño, marca de tiempo del 1 más reciente que contiene). Los
tamaños son potencias de 2 y hay a lo sumo dos buckets de cada tamaño; cuando aparece
un tercero, los dos más antiguos de ese tamaño se fusionan. La estimación suma todos
los buckets salvo la mitad del más antiguo, que puede estar solo parcialmente dentro de
la ventana: el error relativo es como máximo 50 %.

La ventana puede medirse en eventos (los últimos N eventos) o en tiempo (los últimos
N días): basta con que cada evento traiga una marca creciente. Con fechas, varios
eventos comparten el mismo día; eso no cambia la cota, porque la incertidumbre sigue
estando solo en el bucket más antiguo.
"""


class DGIM:
    def __init__(self, window):
        if window < 1:
            raise ValueError('Ventana debe ser positiva')
        self.window = window
        self.time = 0
        self.start = None
        self.buckets = []  # del más reciente al más antiguo: (tamaño, marca)

    def add(self, bit, timestamp=None):
        """Agrega un bit. Sin `timestamp`, la marca es la posición del evento (ventana en eventos)."""
        self.time = self.time + 1 if timestamp is None else timestamp
        self.start = self.time if self.start is None else self.start
        self.buckets = [b for b in self.buckets if b[1] > self.time - self.window]
        if bit:
            self.buckets.insert(0, (1, self.time))
        size = 1
        while True:
            same = [i for i, b in enumerate(self.buckets) if b[0] == size]
            if len(same) <= 2:
                break
            newer, older = same[-2:]
            # El bucket fusionado conserva la marca del más reciente de los dos.
            self.buckets[newer:older + 1] = [(size * 2, self.buckets[newer][1])]
            size *= 2

    def query(self):
        if not self.buckets:
            return 0.0
        total = sum(b[0] for b in self.buckets)
        oldest = self.buckets[-1][0]
        # Exacto si todo el flujo cabe en la ventana o si el bucket más antiguo es un solo 1
        # (su marca está dentro de la ventana, así que cuenta completo).
        if self.time - self.start < self.window or oldest == 1:
            return float(total)
        return total - oldest / 2

    @property
    def bytes(self):
        """Memoria lógica: dos enteros de 8 bytes por bucket."""
        return 16 * len(self.buckets)
