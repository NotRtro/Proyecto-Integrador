**Qué entra al análisis.** De 279 105 registros OCDS, 196 277 procesos (70 %) se convocaron entre 2023 y 2025. El 30 % restante son procesos antiguos (sobre todo 2022, con 18 250) que el OECE vuelve a publicar en los archivos anuales cuando se actualizan. Por eso el año se toma de la fecha de convocatoria y no del nombre del archivo: si no, 2023 aparecería inflado con contrataciones de gestiones anteriores.

**Qué falta y a qué pregunta afecta.**

- El **monto adjudicado** falta en hasta el 25 % de los procesos de un año. No es un error de registro: son procesos sin adjudicación (45 496, el 23 %), es decir, desiertos, cancelados o todavía en curso cuando se descargó el archivo. Las cifras de adjudicación de la Q1 cubren solo lo que llegó a buen pro.
- El **número de postores** falta en hasta el 13 % (2024). Ese vacío limita la Q2: la tasa de postor único se calcula solo sobre procesos informados, y el "sin dato" se reporta aparte por modalidad. En Licitación Pública llega al 24 %, así que la baja tasa de postor único de esa modalidad descansa en tres cuartas partes de sus procesos.
- El registro no trae el departamento del lugar de ejecución, solo el domicilio del comprador. Un ministerio con sede en Lima que construye en Puno suma a Lima.

**Consorcios: el límite más serio para medir concentración.** Los consorcios reciben el 51,5 % del monto atribuido a proveedores, pero en el OCDS figuran con un identificador propio que no revela a las empresas que los integran. Cada consorcio es, para estos datos, un proveedor nuevo. Si las mismas empresas ganan una y otra vez bajo consorcios con distinto nombre, la concentración real es mayor que la medida. La Q1 se presenta por eso con y sin consorcios.

**Dos correcciones que cambian resultados.** Se unificaron 198 RUC que aparecían con varios nombres; sin eso, un mismo proveedor contaba como varios y la concentración bajaba artificialmente. Y 4 216 montos en moneda extranjera se convirtieron a soles con la tasa de la propia convocatoria o con medianas históricas del OECE, sin convertir nunca un faltante en cero.

**Límite pendiente: un procedimiento, varios `ocid`.** La Q3 encontró 15 915 pares de procesos de la misma entidad con la misma nomenclatura (a veces con otra puntuación) y distinto `ocid`. No todos son lo mismo:

- **Copias del mismo registro:** misma fecha y monto, y una de ellas sin adjudicación. El caso extremo es la licitación del hospital de Loreto de 2024, publicada cuatro veces por más de S/ 1 000 millones cada una. Inflan los conteos y el monto convocado.
- **Ítems o lotes de un procedimiento publicados por separado:** montos y ganadores distintos. En el 65 % de los pares adjudicados en ambos lados los proveedores difieren. Son contratos legítimos y deben contarse.

El OCDS no permite separar automáticamente ambos casos. Por eso la capa de datos no los elimina y la Q3 los excluye de sus rankings como "mismo procedimiento". Separarlos exige comparar los ítems de cada copia; es la siguiente mejora del pipeline.
