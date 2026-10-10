#!/usr/bin/env python3
"""
detector.py - CASO B: Analitica de e-commerce en vivo (topic 'ecommerce').

Lee el stream de productor_ecommerce.py y aplica 3 reglas de deteccion con
Spark Structured Streaming:

  1. Productos mas vistos por ventana de tiempo (trending en vivo).
  2. Tasa de conversion vista -> compra por ventana.
  3. Carritos abandonados (carrito sin compra posterior) via stream-stream join.

El evento no trae timestamp propio, asi que se usa el timestamp de ingesta
de Kafka (columna "timestamp" del conector) como event time para las
ventanas y los watermarks.

Ejecucion (dentro del contenedor "spark", ver run_detector.sh):
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 detector.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, sum as _sum, when, round as _round,
    expr, lit,
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "ecommerce"
VENTANA = "1 minute"          # tamano de ventana para ranking y conversion
MAX_ESPERA_COMPRA = "1 minute"   # tiempo que se le da a un carrito para convertirse en compra
WATERMARK_JOIN = "2 minutes"     # tolerancia a eventos tardios en el join de carritos/compras

spark = (
    SparkSession.builder
    .appName("DetectorEcommerce")
    # Default de Spark son 200 particiones de shuffle/estado; en un solo
    # contenedor local eso genera 200 state stores (y 200 lineas de WARN
    # por batch) que ahogan la consola y esconden las alertas [REGLA ...].
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("usuario", StringType()),
    StructField("producto", StringType()),
    StructField("tipo", StringType()),
    StructField("precio", DoubleType()),
])

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

eventos = (
    raw
    .select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("evt_time"),
    )
    .select("data.*", "evt_time")
)

# ---------------------------------------------------------------------------
# REGLA 1: productos mas vistos por ventana (trending en vivo)
# ---------------------------------------------------------------------------
vistas_por_ventana = (
    eventos
    .filter(col("tipo") == "vista")
    .withWatermark("evt_time", "2 minutes")
    .groupBy(window(col("evt_time"), VENTANA), col("producto"))
    .agg(count("*").alias("vistas"))
)


def regla1_top_vistos(df, epoch_id):
    if df.rdd.isEmpty():
        return
    # Se agrupa en Python puro sobre las filas ya traidas al driver: comparar
    # una columna struct ("window") contra un literal en Spark no funciona
    # (SparkRuntimeException: UNSUPPORTED_FEATURE.LITERAL_TYPE), asi que no
    # se vuelve a filtrar con Spark, se agrupa con un dict normal.
    filas = df.collect()
    por_ventana = {}
    for r in filas:
        clave = (r["window"]["start"], r["window"]["end"])
        por_ventana.setdefault(clave, []).append(r)
    for (ini, fin), rows in sorted(por_ventana.items()):
        top5 = sorted(rows, key=lambda r: r["vistas"], reverse=True)[:5]
        print(f"\n[REGLA 1] TOP PRODUCTOS VISTOS  ventana {ini} -> {fin}")
        for i, r in enumerate(top5, start=1):
            print(f"   {i}. {r['producto']:>5}  -> {r['vistas']} vistas")


q1 = (
    vistas_por_ventana.writeStream
    .outputMode("update")
    .foreachBatch(regla1_top_vistos)
    .trigger(processingTime="15 seconds")
    .option("checkpointLocation", "/tmp/chk/regla1")
    .start()
)

# ---------------------------------------------------------------------------
# REGLA 2: tasa de conversion vista -> compra por ventana
# ---------------------------------------------------------------------------
conversion_por_ventana = (
    eventos
    .filter(col("tipo").isin("vista", "compra"))
    .withWatermark("evt_time", "2 minutes")
    .groupBy(window(col("evt_time"), VENTANA))
    .agg(
        _sum(when(col("tipo") == "vista", 1).otherwise(0)).alias("vistas"),
        _sum(when(col("tipo") == "compra", 1).otherwise(0)).alias("compras"),
    )
    .withColumn(
        "tasa_conversion_pct",
        _round(
            when(col("vistas") > 0, col("compras") / col("vistas") * 100).otherwise(lit(0.0)),
            2,
        ),
    )
)


def regla2_conversion(df, epoch_id):
    if df.rdd.isEmpty():
        return
    for r in df.orderBy("window.start").collect():
        ini, fin = r["window"]["start"], r["window"]["end"]
        print(f"\n[REGLA 2] TASA DE CONVERSION  ventana {ini} -> {fin}")
        print(f"   vistas={r['vistas']}  compras={r['compras']}  conversion={r['tasa_conversion_pct']}%")


q2 = (
    conversion_por_ventana.writeStream
    .outputMode("update")
    .foreachBatch(regla2_conversion)
    .trigger(processingTime="15 seconds")
    .option("checkpointLocation", "/tmp/chk/regla2")
    .start()
)

# ---------------------------------------------------------------------------
# REGLA 3: carritos abandonados = carrito sin compra posterior del mismo
# usuario+producto dentro de MAX_ESPERA_COMPRA. Se resuelve con un
# stream-stream LEFT OUTER JOIN (requiere watermark en ambos lados y
# outputMode "append", que es lo unico soportado por joins externos).
# ---------------------------------------------------------------------------
carritos = (
    eventos
    .filter(col("tipo") == "carrito")
    .select(
        col("usuario").alias("c_usuario"),
        col("producto").alias("c_producto"),
        col("evt_time").alias("c_time"),
    )
    .withWatermark("c_time", WATERMARK_JOIN)
)

compras = (
    eventos
    .filter(col("tipo") == "compra")
    .select(
        col("usuario").alias("p_usuario"),
        col("producto").alias("p_producto"),
        col("evt_time").alias("p_time"),
    )
    .withWatermark("p_time", WATERMARK_JOIN)
)

condicion_join = (
    (col("c_usuario") == col("p_usuario"))
    & (col("c_producto") == col("p_producto"))
    & (col("p_time") >= col("c_time"))
    & (col("p_time") <= col("c_time") + expr(f"interval {MAX_ESPERA_COMPRA}"))
)

carritos_con_compra = carritos.join(compras, condicion_join, "leftOuter")

carritos_abandonados = (
    carritos_con_compra
    .filter(col("p_usuario").isNull())
    .select(
        col("c_usuario").alias("usuario"),
        col("c_producto").alias("producto"),
        col("c_time").alias("agregado_en"),
    )
)


def regla3_abandonados(df, epoch_id):
    if df.rdd.isEmpty():
        return
    print(f"\n[REGLA 3] CARRITOS ABANDONADOS detectados en este batch: {df.count()}")
    for r in df.collect():
        print(f"   usuario={r['usuario']}  producto={r['producto']}  agregado_en={r['agregado_en']}")


q3 = (
    carritos_abandonados.writeStream
    .outputMode("append")
    .foreachBatch(regla3_abandonados)
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", "/tmp/chk/regla3")
    .start()
)

print("Detector corriendo. Ctrl+C para detener.")
spark.streams.awaitAnyTermination()
