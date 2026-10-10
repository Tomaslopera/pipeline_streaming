**BIG DATA  ·  TRABAJO 3  (TERCERA NOTA)**

**Pipeline de streaming en tiempo real**

*Detección de patrones en vivo con Kafka \+ Spark Structured Streaming*

Prof. Alejandro Mery Agudelo  ·  En parejas

## **De qué se trata**

Construirán un pipeline de streaming completo que consume un flujo de eventos desde Kafka y detecta patrones en tiempo real usando Spark Structured Streaming. Es el paso final de la unidad: aplicar todo lo visto (Kafka, parseo de eventos, ventanas de tiempo, estado y reglas) a un problema de negocio real, de principio a fin.

| Cómo funciona este trabajo • Elijan UNO de los tres casos de negocio descritos abajo. Los tres tienen nivel de dificultad equivalente; elijan el dominio que más les interese. • El profe entrega el PRODUCTOR de Kafka de su caso (genera el flujo de eventos con los patrones ya sembrados). Ustedes construyen TODO lo demás. • Trabajo en PAREJAS. Esta es la tercera nota formal del curso. |
| :---- |

## **Qué les da el profe / qué construyen ustedes**

**Les entrega el profe:** el script productor del caso (p. ej. productor\_flota.py), que envía los eventos a Kafka. No lo modifiquen; es su fuente de datos.

**Construyen ustedes:**

* El montaje de Kafka en Docker y la creación del topic (como en la Clase 12).

* El detector con Spark Structured Streaming: leer de Kafka, parsear el JSON, e implementar las 3 reglas de detección de su caso.

* El informe de negocio: interpretar lo que detectan, no solo producir alertas.

* La evidencia de que el pipeline corrió en vivo (capturas del detector alertando).

## **Requisitos técnicos (comunes a los tres casos)**

* Kafka levantado en Docker (reutilicen el entorno de la Clase 12).

* Spark leyendo de Kafka con el conector spark-sql-kafka (se descarga al arrancar).

* Spark y Kafka en la MISMA máquina (Colab en la nube no ve su Kafka local).

**Nota técnica (aplica sobre todo al Caso C, y a la regla 2 del Caso A)**

foreachBatch en caso de problemas con las windows functions ;)

| CASO A | Monitoreo de una flota de vehículos *Topic de Kafka: flota* |
| :---: | :---- |

Una empresa de logística monitorea su flota en tiempo real. Cada vehículo emite telemetría continuamente: velocidad, nivel de combustible y posición. Deben construir un tablero de alertas que detecte problemas al instante.

**Cada evento trae:** id\_vehiculo, velocidad, combustible, lat, lon.

## **Reglas a implementar (las 3\)**

1. EXCESO DE VELOCIDAD SOSTENIDO: alertar cuando un vehículo supera los 100 km/h en 3 o más lecturas dentro de una ventana de tiempo (no un pico aislado, sino sostenido).

2. CAÍDA ANÓMALA DE COMBUSTIBLE: alertar cuando el combustible de un vehículo baja mucho más rápido de lo normal entre lecturas (posible robo o fuga). Pista: necesitan comparar cada lectura con la anterior del mismo vehículo.

3. VEHÍCULO EN SILENCIO: detectar vehículos que venían emitiendo y dejaron de hacerlo (posible falla o accidente). Pista: rastreen la última emisión de cada vehículo.

**Preguntas de negocio:** ¿qué vehículo dio más alertas? ¿el silencio se detecta rápido o tarde? ¿qué harían en producción con cada tipo de alerta?

| CASO B | Analítica de e-commerce en vivo *Topic de Kafka: ecommerce* |
| :---: | :---- |

Una tienda online quiere entender el comportamiento de sus usuarios EN VIVO, no con reportes del día siguiente. Cada acción de los usuarios genera un evento: ver un producto, agregarlo al carrito o comprarlo. Construyan un panel de analítica en tiempo real.

**Cada evento trae:** usuario, producto, tipo (vista / carrito / compra), precio.

## **Reglas a implementar (las 3\)**

1. PRODUCTOS MÁS VISTOS POR VENTANA: calcular, en ventanas de tiempo, el ranking de productos más vistos. Es el 'trending' de la tienda en vivo.

2. TASA DE CONVERSIÓN: por ventana, calcular qué porcentaje de las vistas terminan en compra (compras / vistas). Es la métrica reina del e-commerce.

3. CARRITOS ABANDONADOS: detectar usuarios que agregaron un producto al carrito pero NO lo compraron (un evento que no llegó). Pista: comparen quién agregó vs. quién compró.

**Preguntas de negocio:** ¿cuál es el producto estrella? ¿la tasa de conversión es sana? ¿cuántos carritos se abandonan y qué campaña propondrían para recuperarlos?

| CASO C | Monitoreo de precios de acciones/cripto *Topic de Kafka: precios* |
| :---: | :---- |

Una plataforma de inversión monitorea precios de varios activos (acciones y cripto) que cambian constantemente. Construyan un sistema que calcule indicadores en vivo y genere señales de trading, como lo haría un panel financiero real.

**Cada evento trae:** simbolo, precio.

## **Reglas a implementar (las 3\)**

1. MEDIAS MÓVILES: para cada símbolo, calcular una media móvil corta (p. ej. últimos 3 precios) y una larga (p. ej. últimos 8). Es la base del análisis técnico.

2. VOLATILIDAD: alertar cuando un símbolo tiene un cambio de precio brusco (p. ej. rango \> 3% dentro de una ventana). Señala movimientos importantes del mercado.

3. CRUCE DE MEDIAS: generar una señal de COMPRA cuando la media corta cruza por encima de la larga (y de VENTA en el cruce contrario). Es una estrategia clásica de trading.

**Preguntas de negocio:** ¿qué símbolo fue más volátil? ¿cuántas señales de compra/venta se generaron? ¿confiarían en esta estrategia con dinero real? ¿por qué sí o no?

# **Qué y cómo entregar**

1. El código de su detector (detector.py), con las 3 reglas de su caso implementadas y comentadas.

2. Un informe en PDF o .md (máx. 5 páginas) con: qué caso eligieron, cómo funciona su detector, los resultados de cada regla, las respuestas a las preguntas de negocio, y al menos una captura del detector alertando EN VIVO (evidencia de que el pipeline corrió).

3. Una breve sección de 'retos técnicos': qué les costó (montar Kafka, el conector, foreachBatch...) y cómo lo resolvieron.

4. Los nombres de la pareja y el caso elegido en la primera página.

ENTREGA 14 de OCT