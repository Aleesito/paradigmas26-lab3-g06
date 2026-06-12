# Informe — Laboratorio 3: Procesamiento distribuido con Apache Spark

**Materia:** Paradigmas de Programación 2026 — FAMAF  
**Grupo:** g06  
**Integrantes:**
- Lopez Benavides Francisco
- Brizuela Franco
- Prieto Ale
- Hernandez Juan Martin

---

## Decisiones de diseño relevantes

> Describir qué modificaciones se hicieron respecto al esqueleto de los laboratorios anteriores y por qué. Incluir: cambios en la estructura de clases, en el flujo de ejecución, en el manejo de errores, etc.

<!-- Completar -->

---

## Ejercicio 1 — Identificar las regiones paralelizables

### a) Diagrama de flujo del pipeline

> Dibujar o describir el grafo de dependencias del pipeline completo, indicando para cada paso su input y output con los tipos Scala correspondientes.

**Pasos del pipeline:**

| Paso | Descripción | Tipo de entrada | Tipo de salida |
|------|-------------|-----------------|----------------|
| 1 | Lectura de suscripciones | `String` (ruta archivo) | `List[Subscription]` |
| 2 | Descarga de feeds | `Subscription` | `Iterator[Post]` |
| 3 | Filtrado de posts vacíos | `RDD[Post]` | `RDD[Post]` |
| 4 | Extracción de entidades nombradas | `Post` | `Iterator[NamedEntity]` |
| 5 | Conteo por entidad | `NamedEntity` | `((String, String), Int)` |
| 6 | Reducción / agregación | `((String, String), Int)` | `((String, String), Int)` |
| 7 | Ordenamiento y presentación | `RDD[((String, String), Int)]` | Salida por pantalla |

<!-- Opcional: agregar un diagrama ASCII o imagen del grafo -->

### b) Clasificación de pasos según abstracciones de Spark

> Para cada paso del pipeline, indicar si corresponde a `map`, `flatMap`, `reduceByKey` u otra abstracción, y justificar.

| Paso | Abstracción Spark | Justificación |
|------|-------------------|---------------|
| Descarga de feeds | `flatMap` | <!-- Completar --> |
| Filtrado de posts | `filter` | <!-- Completar --> |
| Extracción de entidades | `flatMap` | <!-- Completar --> |
| Generación de pares clave-valor | `map` | <!-- Completar --> |
| Conteo de entidades | `reduceByKey` | <!-- Completar --> |
| Ordenamiento | `sortBy` / `collect` | <!-- Completar --> |

**¿Hay algún paso que no encaje en ninguna abstracción?**

<!-- Completar: identificar pasos que requieran acciones del driver (collect, count, etc.) y explicar por qué no pueden expresarse como transformaciones puras -->

### c) Barreras de sincronización

> Identificar qué pasos constituyen una barrera de sincronización y cuáles pueden ejecutarse de forma completamente independiente entre workers.

**Pasos con barrera de sincronización:**

<!-- Completar: indicar cuáles son y por qué (ej: reduceByKey, collect, count) -->

**Pasos completamente independientes entre workers:**

<!-- Completar: indicar cuáles son (ej: map, flatMap sobre cada elemento) -->

### d) Restricciones sobre las funciones pasadas a Spark

> ¿Qué restricciones impone Spark sobre las funciones que el desarrollador pasa a cada transformación?

**Serialización:**

<!-- Completar: toda función y los datos que captura deben ser serializables (implementar Serializable o usar tipos primitivos/case classes) para poder enviarse a los workers -->

**Estado compartido:**

<!-- Completar: las funciones no deben depender ni modificar estado compartido mutable entre workers; los únicos mecanismos seguros son Accumulators (solo escritura en workers) y Broadcast variables (solo lectura en workers) -->

**Efectos secundarios:**

<!-- Completar: las funciones deben ser lo más puras posibles; efectos secundarios como I/O pueden ejecutarse más de una vez ante fallos y re-ejecuciones de tareas -->

---

## Ejercicio 2 — Paralelizar la descarga de feeds

### ¿Qué pasaría si se dejara propagar la excepción dentro del flatMap en lugar de manejarla internamente?

Si una excepción se propaga fuera de la función pasada al flatMap, Spark marca la tarea como fallida y la reintenta. Si sigue fallando, cancela el stage completo y el job entero falla con una excepción. Esto significa que un solo feed inalcanzable haría fallar todo el programa, perdiendo el trabajo de todos los demás feeds ya descargados.

Al manejar el error internamente capturando la excepción el worker simplemente emite cero posts para ese feed y continúa con los demás. El pipeline sigue con los feeds que sí funcionaron.


---

## Ejercicio 3 — Paralelizar el cómputo de entidades nombradas

### `reduceByKey` como barrera de sincronización

> ¿Qué ocurre en el cluster en el punto de `reduceByKey`? ¿Por qué es inevitable para este problema?

<!-- Completar: reduceByKey produce un shuffle: Spark redistribuye todos los pares (clave, valor) de forma que todos los pares con la misma clave queden en el mismo worker. Es inevitable porque para contar apariciones totales de una entidad es necesario combinar los conteos parciales de todos los workers que hayan encontrado esa entidad -->

### Restricciones de la función pasada a `reduceByKey`

> ¿Qué restricciones debe cumplir la función que se le pasa a `reduceByKey`? Pensar en conmutatividad y asociatividad.

<!-- Completar: la función debe ser (1) asociativa: f(f(a,b),c) == f(a,f(b,c)), para que Spark pueda combinar parcialmente resultados en cualquier orden; y (2) conmutativa: f(a,b) == f(b,a), para que el orden en que lleguen los valores no afecte el resultado. La suma de enteros cumple ambas propiedades -->

### ¿Dónde se lee el diccionario de entidades?

> ¿La lectura del diccionario de entidades ocurre en el driver o en los workers?

<!-- Completar: indicar dónde se carga el diccionario en la implementación actual y qué implicancias tiene. Si se carga dentro de un flatMap, se carga en cada worker (y posiblemente múltiples veces). Si se carga en el driver y se pasa como Broadcast variable, se serializa y envía una sola vez a cada worker -->

---

## Ejercicio 4 — Monitoreo del éxito de las tareas

### Limitaciones de los Accumulators para tomar decisiones lógicas

> ¿Por qué los Accumulators solo deben usarse para métricas y no para tomar decisiones lógicas dentro de las etapas distribuidas del pipeline? ¿En qué situación puede dar un valor incorrecto?

<!-- Completar: Spark puede re-ejecutar tareas ante fallos (o especulativamente), lo que haría que un Accumulator se incremente más de una vez para el mismo dato. Por eso su valor puede ser impreciso durante la ejecución. Usarlos para controlar flujo lógico (ej: "si hay más de N errores, abortar") sería incorrecto -->

### ¿Cuándo está disponible el valor de un Accumulator para el driver?

> ¿En qué momento del pipeline está disponible el valor de un Accumulator para ser leído por el driver?

<!-- Completar: el valor de un Accumulator solo es confiable después de que una acción terminal (collect, count, saveAsTextFile, etc.) haya completado. Durante la evaluación lazy de transformaciones, los valores aún no están disponibles -->

### Comparativa de tiempos: versión secuencial vs. Spark

> Comparar el tiempo de ejecución de cada etapa entre la versión sin paralelización (esqueleto) y la versión con Spark.

| Etapa | Tiempo secuencial (ms) | Tiempo con Spark (ms) | Observaciones |
|-------|------------------------|----------------------|---------------|
| Descarga de feeds | <!-- --> | <!-- --> | <!-- --> |
| Extracción de entidades | <!-- --> | <!-- --> | <!-- --> |
| Reducción / conteo | <!-- --> | <!-- --> | <!-- --> |
| **Total** | <!-- --> | <!-- --> | <!-- --> |

**Conclusiones:**

<!-- Completar: para el volumen de datos del laboratorio, ¿se aprecia diferencia? Discutir el overhead de inicialización de Spark (SparkContext, serialización, scheduling) y si justifica la paralelización para inputs pequeños vs. grandes -->

### Captura de Spark UI

> Incluir captura de pantalla de la Spark UI mostrando el grafo de ejecución y el progreso de cada stage.

<!-- Insertar imagen: ![Spark UI](./spark-ui-screenshot.png) -->

**Análisis de la Spark UI:**

<!-- Completar: describir qué stages se observan, cuántas tareas por stage, tiempos, y si hay algún stage que tome significativamente más tiempo -->

---

## Ejercicio 5 — Acceso a datos y persistencia de RDDs

### ¿Qué ocurriría sin `cache()`? ¿Cuántas veces se ejecutaría la descarga de feeds si no se llamara a `cache()`?

Sin `cache()`, cada acción sobre `filteredPosts` recomputaría todo el recorrido desde el principio: volvería a leer las suscripciones, descargar todos los feeds, parsear los posts y filtrar. En nuestro pipeline hay dos acciones sobre `filteredPosts` (count() y map(...).sum()), por lo que los feeds se descargarían dos veces. Con cache(), la primera acción materializa y almacena el RDD en memoria. La segunda acción lo reutiliza directamente sin recomputar.

### ¿Por qué es incorrecto llamar a collect() entre los pasos a) y b) del ejercicio 3 y continuar el pipeline? ¿Qué consecuencia tiene sobre la distribución del trabajo?

Llamar a `collect()` trae todos los datos al driver y devuelve un Array. Si se continua el pipeline a partir de ese punto, se opera sobre una colección local en el driver, no sobre un RDD. Cualquier transformación posterior (map, reduceByKey, etc.) se ejecutaría secuencialmente en el driver, sin distribución. Se pierde completamente la paralelización, el mayor beneficio de usar Spark. 



### `cache()` es lazy: ¿En qué momento se almacena realmente el RDD en memoria?

Notamos que `cache()` solo marca el RDD como "persistir cuando se materialice". El almacenamiento real ocurre cuando la primera acción sobre ese RDD se ejecuta. En nuestro código:

```scala
filteredPosts.cache()           // solo marca, no ejecuta nada
...
val postCount = filteredPosts.count()  // acá se materializa y se guarda en memoria
```
Las acciones subsiguientes sobre ese RDD leen directamente de la caché.

---

## Referencias

<!-- Opcional: listar documentación, artículos o recursos consultados -->

- [Documentación oficial de Apache Spark](https://spark.apache.org/docs/latest/)
- [Spark Programming Guide — RDDs](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
- [Spark Accumulators](https://spark.apache.org/docs/latest/rdd-programming-guide.html#accumulators)