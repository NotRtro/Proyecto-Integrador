**De pares a temas.** LSH entrega 130 mil pares casi idénticos, que leídos uno a uno no dicen qué se repite. Al unirlos en grupos (componentes conexas, sin contar los pares del mismo procedimiento) quedan **5 947 grupos de 3 o más procesos que reúnen 23 874 procesos**. Cada grupo lleva como etiqueta las palabras que mejor lo distinguen del resto del corpus. Las etiquetas salen de los datos, sin ninguna lista escrita a mano, y se leen solas.

**Dos tipos de grupos, con lecturas opuestas:**

- **Una sola entidad (5 758 grupos, 97 %).** Los más grandes son todos del OEFA, y la etiqueta separa cada perfil: "titulado · supervisor · tercero · ingeniería" (181 procesos), "bachiller · fiscalizador · derecho" (123), "química · supervisor", "biología · titulado", "economía · fiscalizador". La etiqueta convierte la repetición en una lista de puestos: el OEFA arma su personal de campo con contratos de servicios por perfil profesional, uno por persona. Lo mismo hace el programa Educación Básica para Todos con los concursos de la reforma magisterial (64 procesos) y la red de salud Morropón–Chulucanas con sus establecimientos (27).
- **Varias entidades (189 grupos).** Aquí está la oportunidad de política: el mismo bien, descrito casi igual, comprado por separado. **Cemento portland puzolánico** por 43 entidades (83 procesos), **cemento tipo I** por 42, **diésel** por 31 y 25 entidades, **leche evaporada para el Vaso de Leche** por 15–18 municipalidades, **mandiles** para la salud del Callao, **seguridad y vigilancia** (13 entidades), **seguro complementario de trabajo de riesgo (SCTR)** (12), **telefonía móvil** (9) y **eritropoyetina** para hospitales (7). Son bienes y servicios estándar que un acuerdo marco o una compra corporativa podría agregar. Coinciden con los códigos más frecuentes del EDA de ítems.

**Cómo se calcula la etiqueta.** Cada palabra recibe el puntaje (fracción de procesos del grupo que la usan) × log(N / procesos del corpus que la usan), y se queda con las cuatro mejores entre las que aparecen en al menos la mitad del grupo. Así "cemento" o "tercero" ganan, porque son frecuentes en el grupo y raras en el corpus, mientras que "servicio" o "adquisición" pierden, porque están en todas partes.

**Límites.**

- El enlace simple encadena: un grupo puede juntar procesos que se parecen poco entre sí si los une una cadena de pares. Por eso se muestra el Jaccard medio de sus aristas, que en los grupos grandes está entre 0,77 y 1.
- Los textos-plantilla compartidos por más de 200 procesos no se expanden en pares (ver el corpus) y no aparecen aquí como grupo.
- La etiqueta describe el texto, no el expediente.
