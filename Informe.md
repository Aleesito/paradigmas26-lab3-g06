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
 

---

## Ejercicio 2 — Paralelizar la descarga de feeds

### Manejo de errores dentro del flatMap

> ¿Qué pasaría si se dejara propagar la excepción dentro del flatMap en lugar de manejarla internamente?

<!-- Completar: si una excepción se propaga fuera de la función de un flatMap, Spark cancela la tarea completa. Dependiendo de la configuración de reintentos, puede reintentar la tarea o abortar el stage/job entero, lo que impediría procesar el resto de los feeds -->

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

### ¿Qué ocurriría sin `cache()`?

> ¿Cuántas veces se ejecutaría la descarga de feeds si no se llamara a `cache()`?

<!-- Completar: sin cache(), cada acción que dependa del RDD de posts recomputará el pipeline completo desde el principio, incluyendo las descargas HTTP. Si hay N acciones sobre el RDD de posts, los feeds se descargarían N veces -->

### Por qué es incorrecto llamar a `collect()` entre los pasos del ejercicio 3

> ¿Por qué es incorrecto llamar a `collect()` entre los pasos a) y b) del ejercicio 3 y luego continuar el pipeline? ¿Qué consecuencia tiene sobre la distribución del trabajo?

<!-- Completar: collect() trae todos los datos al driver, rompiendo la distribución. Si luego se continúa el pipeline desde el driver, el procesamiento subsiguiente es secuencial y ya no se beneficia de la paralelización de Spark. Además, si el volumen de datos es grande, puede causar un OutOfMemoryError en el driver -->

### `cache()` es lazy: ¿cuándo se materializa el RDD?

> `cache()` es también lazy. ¿En qué momento se almacena realmente el RDD en memoria?

<!-- Completar: cache() solo marca el RDD para ser persistido; no dispara computación. El RDD se materializa y almacena en memoria la primera vez que se ejecuta una acción que lo requiere. Las acciones subsiguientes sobre ese RDD lo leen directamente de la caché -->

---

## Referencias

<!-- Opcional: listar documentación, artículos o recursos consultados -->

- [Documentación oficial de Apache Spark](https://spark.apache.org/docs/latest/)
- [Spark Programming Guide — RDDs](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
- [Spark Accumulators](https://spark.apache.org/docs/latest/rdd-programming-guide.html#accumulators)