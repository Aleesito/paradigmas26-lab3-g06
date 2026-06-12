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

<!-- Completar -->

---

## Ejercicio 1 — Identificar las regiones paralelizables

### a) Diagrama de flujo del pipeline
```
[subscriptions.json]    (o cualquier .json)
           |
           | String (ruta del archivo)
           v
  1. Leer suscripciones
     (FileIO.readSubscriptions)
           |
           | List[Option[Subscription]]  →  flatten  →  List[Subscription]
           v
  2. sc.parallelize + descargar feeds (HTTP)
     (FileIO.downloadFeed — dentro de flatMap sobre RDD[Subscription])
           |
           | RDD[Post]   (todos los posts de todos los feeds)
           v
  3. Parsear posts de cada feed
     (JsonParser.parsePosts — dentro del mismo flatMap)
           |
           | RDD[Post]   (posts parseados)
           v
  4. Filtrar posts vacíos
     (Analyzer.isEmptyPost — filter sobre RDD[Post])
           |
           | RDD[Post]   (posts válidos)   ← .cache()
           v
  5. Detectar entidades en cada post
     (Analyzer.detectEntities — flatMap sobre RDD[Post])
           |
           | RDD[NamedEntity]   (todas las entidades de todos los posts)
           v
  6. Contar entidades  [barrera: shuffle]
     (map → ((entityType, text), 1)  luego  reduceByKey(_ + _))
           |
           | RDD[((String, String), Int)]   ← .cache()
           v
  7. Ordenar y mostrar el ranking
     (sortBy + take(topK) → Formatters.formatEntityStats
      reduceByKey por tipo → Formatters.formatTypeStats)
           |
           | String (salida por consola)
           v
        [stdout]
```


### b) Clasificación de pasos según abstracciones de Spark

| Paso | Abstracción Spark | Justificación |
|------|-------------------|---------------|
| Leer Subscripciones | No aplica | Se ejecuta en el driver antes de crear algun RDD, el resultado de este paso es luego la estructura de datos que vamos a paralelizar |
| Descarga de feeds y Parseo | `flatMap` | Cada Subscription genera 0 o algun post, estos luego se parsean individualmente. Cada uno es independiente entre si |
| Filtrado de posts | `flatMap` | En este caso se filtran aquellos posts vacios (genera una cantida nula) o se dejan, cada post se evalua independientemente |
| Deteccion de entidades | `flatMap` | El procesamiento de cada post individualmente genera (detecta) una cantidad variable de entidades |
| Conteo de entidades | `map` / `reduceByKey` | Se transforma cada entidad en un par (entidad, cantidad), para luego combinar entidades semejantes y sumar sus cantidades |
| Ordenamiento y Estadisticas | No aplica | A pesar de poder realizar un ordenamiento parcial entre las partes, se necesitan los datos finales (de todos los workers) para ordenar y posteriormente mostrar por pantalla las estadisticas. No tiene sentido aqui la paralelización|

**¿Hay algún paso que no encaje en ninguna abstracción?**

Sí, varios. La lectura de suscripciones ocurre íntegramente en el driver antes de que exista un RDD. El `count()` sobre `filteredPosts`, el `take(topK)` sobre el RDD ordenado, y el `collect()` para construir `typeStats` son **acciones**, no transformaciones: no producen un nuevo RDD sino que extraen datos al driver. Estos pasos son necesarios porque el programa necesita tomar decisiones de control de flujo (verificar si `postCount == 0`) o producir la salida final, cosas que solo el driver puede hacer.


### c) Barreras de sincronización

**Pasos con barrera de sincronización:**
 
- `reduceByKey(_ + _)` para el conteo de entidades: para combinar los conteos parciales de todos los workers, Spark necesita redistribuir los pares clave-valor de forma que todos los pares con la misma clave queden en la misma partición. Esto produce un **shuffle**: ningún worker puede producir su resultado parcial hasta haber recibido todos los pares que le corresponden. Lo mismo ocurre con el segundo `reduceByKey` que agrega conteos por tipo.
- `sortBy`: requiere un shuffle global para ordenar el RDD entre particiones.
- `count()`, `take()`, `collect()`: son acciones que obligan al driver a esperar a que todas las tareas de todos los stages anteriores hayan completado antes de devolver un valor. Son barreras absolutas: nada posterior puede comenzar hasta que terminen.
**Pasos completamente independientes entre workers:**
 
- `flatMap` de descarga y parseo: cada worker procesa su `Subscription` de forma totalmente aislada. El fallo de un feed no afecta al resto.
- `filter` de posts vacíos: cada worker evalúa sus `Post` localmente, sin necesitar datos de otras particiones.
- `flatMap` de detección de entidades: cada worker aplica `detectEntities` a sus posts de forma independiente.
- `map` que genera los pares `((entityType, text), 1)`: transformación uno a uno, completamente local por elemento.


**Pasos completamente independientes entre workers:**

- `flatMap` de descarga y parseo: cada worker procesa su `Subscription` de forma totalmente aislada. El fallo de un feed no afecta al resto.
- `filter` de posts vacíos: cada worker evalúa sus `Post` localmente, sin necesitar datos de otras particiones.
- `flatMap` de detección de entidades: cada worker aplica `detectEntities` a sus posts de forma independiente.
- `map` que genera los pares `((entityType, text), 1)`: transformación uno a uno, completamente local por elemento.


### d) Restricciones sobre las funciones pasadas a Spark


**Serialización:**
 
Toda función que se pase a una transformación de Spark (`flatMap`, `map`, `filter`, etc.) se serializa y se envía a los workers a través de la red. Esto implica que tanto la función en sí como cualquier objeto que capture del scope externo deben ser serializables. En nuestro proyecto, `NamedEntity` extiende `Serializable` explícitamente por esta razón. El `dictionary` que se captura dentro del `flatMap` de detección de entidades es una `List[NamedEntity]` que también debe ser serializable para poder enviarse a cada worker. Si un objeto capturado no es serializable, Spark lanza una `NotSerializableException` en tiempo de ejecución.
 
**Estado compartido:**
 
Las funciones distribuidas no deben leer ni escribir estado mutable compartido entre workers. El acceso concurrente desde múltiples workers a una variable externa produciría condiciones de carrera y resultados incorrectos. Los únicos mecanismos seguros que provee Spark son los **Accumulators** (los workers solo pueden incrementarlos, el driver solo puede leerlos) y las **Broadcast variables** (el driver serializa el valor una sola vez y los workers solo pueden leerlo). En nuestro código, los cinco `LongAccumulator` siguen este contrato correctamente.
 
**Efectos secundarios:**
 
Las funciones pasadas a Spark deben ser lo más puras posible. Spark puede re-ejecutar una tarea ante un fallo o en modo especulativo, lo que significa que la misma función puede ejecutarse más de una vez sobre el mismo dato. Los `Console.err.println` dentro del `flatMap` de descarga son un ejemplo de efecto secundario tolerable (logging), pero si un efecto secundario fuera una escritura a una base de datos o un incremento de contador externo, podría ejecutarse múltiples veces y producir resultados incorrectos. Por la misma razón, los `Accumulator` pueden sobrecontar si una tarea se re-ejecuta, por eso no deben usarse para tomar decisiones lógicas del pipeline.
 

## Ejercicio 2 — Paralelizar la descarga de feeds

### ¿Qué pasaría si se dejara propagar la excepción dentro del flatMap en lugar de manejarla internamente?

Si una excepción se propaga fuera de la función pasada al flatMap, Spark marca la tarea como fallida y la reintenta. Si sigue fallando, cancela el stage completo y el job entero falla con una excepción. Esto significa que un solo feed inalcanzable haría fallar todo el programa, perdiendo el trabajo de todos los demás feeds ya descargados.

Al manejar el error internamente capturando la excepción el worker simplemente emite cero posts para ese feed y continúa con los demás. El pipeline sigue con los feeds que sí funcionaron.


---

## Ejercicio 3 — Paralelizar el cómputo de entidades nombradas

### `reduceByKey` como barrera de sincronización

> ¿Qué ocurre en el cluster en el punto de `reduceByKey`? ¿Por qué es inevitable para este problema?

En este punto, al querer contar la cantidad de entidades detectadas, un **worker** pudo haber encontrado *"Juan"* y un **worker** distinto otro *"Juan"*, para que la suma de entidades sea la correcta y se detecten a todos los *"Juan"* de todos los **worker**, `reduceByKey` produce un **Shuffle**, esto hace que todos los **workers** se detengan un momento y se envíen los datos entre ellos para asegurarse de que todas las entidades con el mismo nombre se agrupen juntas en un mismo **worker** antes de hacer la suma final y enviar el resultado al driver. Este **Shuffle** es inevitable ya que para poder tener el conteo total de todas las entidades es necesario combinar los conteos parciales de todos los **workers** que hayan encontrado esa entidad.

### Restricciones de la función pasada a `reduceByKey`

> ¿Qué restricciones debe cumplir la función que se le pasa a `reduceByKey`? Pensar en conmutatividad y asociatividad.

La función debe ser asociativa, para que Spark pueda combinar parcialmente resultados en cualquier orden; y conmutativa, para que el orden en que lleguen los valores no afecte el resultado. La suma de enteros cumple ambas propiedades.

### ¿Dónde se lee el diccionario de entidades?

> ¿La lectura del diccionario de entidades ocurre en el driver o en los workers?

En nuestra implementación, la lectura del diccionario desde el disco ocurre en el Driver, ya que la función `Dictionary.loadAll()` se invoca en el flujo principal del programa, fuera de cualquier transformación de Spark.

Como la variable `dictionary` es referenciada posteriormente dentro de la función `flatMap`, Spark captura esta variable en la **clausura (closure)** de la función. Para que los workers puedan utilizarla, el Driver serializa la lista del diccionario y la envía a través de los hilos hacia la memoria de cada worker.

Esta decisión de diseño evita que cada worker tenga que realizar operaciones lentas de I/O de forma redundante y concurrente, garantizando que ya tengan la estructura en memoria lista para procesar su partición de datos.

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