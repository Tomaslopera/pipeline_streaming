# Pipeline de streaming en tiempo real — Caso B: E-commerce en vivo

**Big Data — Trabajo 3 (tercera nota)**
**Pareja:** Tomás Lopera y Pedro Sierra

Pipeline de streaming con **Kafka + Spark Structured Streaming** que detecta
en vivo 3 patrones de comportamiento de usuarios en una tienda online
(topic `ecommerce`): productos más vistos por ventana, tasa de conversión
vista→compra, y carritos abandonados.

El informe completo (arquitectura, resultados, preguntas de negocio y retos
técnicos) está en [`informe.md`](informe.md).

---

## Evidencia de ejecución en vivo

**Detector alertando — Reglas 1 y 2 (productos vistos y conversión):**

![Regla 1 y Regla 2 en vivo](images/regla1-2.png)

**Detector alertando — Regla 3 (carritos abandonados):**

![Regla 3 en vivo](images/regla3.png)

**Productor enviando eventos al topic `ecommerce` (terminal paralela):**

![Productor corriendo](images/tickets.png)

**Contenedores de Docker arriba, topic creado con 3 particiones, y offsets
subiendo (evidencia de que Kafka está recibiendo mensajes reales):**

![Docker, topic y offsets](images/docker.png)

---

## Arquitectura

Kafka y Spark corren **ambos en Docker** (mismo host). El único proceso que
corre fuera de Docker es el productor (`productor_ecommerce.py`, entregado
por el profesor y sin modificar), porque viene con `localhost:9092` fijo en
el código — por eso Kafka expone ese puerto al host.

| Archivo | Qué es |
|---|---|
| `productor_ecommerce.py` | Productor Kafka del profesor (no modificar) |
| `docker-compose.yml` | Levanta Kafka (KRaft) + Spark en Docker |
| `detector.py` | El detector: las 3 reglas con Spark Structured Streaming |
| `run_detector.sh` | Lanza `spark-submit` dentro del contenedor `spark` |
| `requirements.txt` | Dependencia (`kafka-python`) para correr el productor en el host |
| `informe.md` | Informe de negocio con resultados y evidencia |

## 0. Requisitos
- Docker Desktop instalado y corriendo (`docker info` no debe dar error).
- Python 3 en el host con `pip install -r requirements.txt` (solo para el productor).

## 1. Levantar Kafka + Spark y crear el topic

```bash
docker compose up -d
```

Esto:
- Levanta `kafka` (modo KRaft, sin Zookeeper).
- Corre `kafka-init`, que crea el topic `ecommerce` (3 particiones) en cuanto Kafka esté sano, y lo describe por consola.
- Levanta `spark` (contenedor bitnami/spark, queda esperando con `tail -f /dev/null`).

Revisa que el topic quedó creado:

```bash
docker compose logs kafka-init
```

## 2. Instalar dependencias del productor (una sola vez, en el host)

```bash
pip install -r requirements.txt
```

## 3. Arrancar el productor (terminal 1, en el host)

```bash
python3 productor_ecommerce.py
```

Déjalo corriendo. Va a imprimir "tick N: eventos enviados" cada segundo.

## 4. Arrancar el detector (terminal 2)

```bash
./run_detector.sh
```

La primera vez va a demorar un poco porque `spark-submit --packages` descarga
el conector `spark-sql-kafka-0-10` y sus dependencias (kafka-clients,
commons-pool2, etc.) dentro del contenedor.

Vas a ver en consola, intercalado:
- `[REGLA 1] TOP PRODUCTOS VISTOS` cada ventana de 1 minuto.
- `[REGLA 2] TASA DE CONVERSION` cada ventana de 1 minuto.
- `[REGLA 3] CARRITOS ABANDONADOS` cada ~30s, con un retraso natural de 1-3
  minutos (hay que esperar la ventana de watermark para estar seguros de que
  el carrito no se convirtió en compra).

Déjalo corriendo unos 5-10 minutos para que se vean varias ventanas y al
menos un lote de carritos abandonados.

## 5. Parar todo

```bash
# Ctrl+C en terminal 1 (productor) y terminal 2 (detector, o Ctrl+C dentro del exec)
docker compose down
```

## Troubleshooting rápido

- **`docker compose: unknown command`** → usa `docker-compose` (guion), el
  binario standalone (`docker-compose version`). `run_detector.sh` ya
  detecta esto solo.
- **El productor no conecta a `localhost:9092`** → confirma que
  `docker compose ps` muestra `kafka` como `healthy` y que el puerto `9092`
  está mapeado (`docker compose port kafka 9095` debe devolver `0.0.0.0:9092`).
- **`spark-submit` no encuentra el topic / timeout** → revisa que
  `kafka-init` terminó sin error (`docker compose logs kafka-init`) antes de
  lanzar el detector.
- **Las imágenes `bitnami/kafka` o `bitnami/spark` no se descargan** → ya no
  tienen tags públicos en Docker Hub; este repo usa `bitnamilegacy/kafka` y
  `bitnamilegacy/spark`, que sí son gratuitas.
