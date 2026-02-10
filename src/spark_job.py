import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, sum as _sum, window, to_timestamp, current_timestamp,
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# ===== 설정 (flink_job.py와 동일) =====
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "payment-log")

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "streamdb")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")

WATERMARK_SECONDS = int(os.environ.get("WATERMARK_SECONDS", "5"))
WINDOW_SECONDS = int(os.environ.get("WINDOW_SECONDS", "10"))

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# 토픽이 아직 없을 때 재시도 설정
MAX_RETRIES = 30
RETRY_INTERVAL = 5


def write_to_postgres(batch_df, batch_id):
    """각 마이크로배치를 PostgreSQL spark_results 테이블에 저장"""
    if batch_df.isEmpty():
        return

    # window.start, window.end, user_id, total_amount, updated_at 선택
    output_df = (
        batch_df
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("user_id"),
            col("total_amount"),
        )
        .withColumn("updated_at", current_timestamp())
    )

    jdbc_properties = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    # PostgreSQL에 append 모드로 쓰기
    output_df.write.jdbc(
        url=JDBC_URL,
        table="spark_results",
        mode="append",
        properties=jdbc_properties,
    )

    print(f"[INFO] Batch {batch_id}: PostgreSQL 저장 완료")


def main():
    print("[INFO] Spark Structured Streaming Job 시작")
    print(f"[INFO] Kafka: servers={KAFKA_BOOTSTRAP_SERVERS} topic={KAFKA_TOPIC}")
    print(f"[INFO] Postgres JDBC URL: {JDBC_URL}")
    print(
        f"[INFO] Event Time Window={WINDOW_SECONDS}s Watermark={WATERMARK_SECONDS}s "
        f"Trigger=1s (Micro-batch)"
    )

    spark = (
        SparkSession.builder
        .appName("SparkStreamingPayment")
        .getOrCreate()
    )

    # Spark 로그 레벨 조정 (불필요한 INFO 줄이기)
    spark.sparkContext.setLogLevel("WARN")

    # JSON 스키마 정의 (user_id, amount, event_time)
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("amount", IntegerType(), True),
        StructField("event_time", StringType(), True),
    ])

    # 왜: Kafka 토픽이 아직 생성되지 않았을 수 있다 (Producer가 먼저 메시지를 보내야 생성됨).
    # Flink는 토픽이 없어도 대기하지만 Spark는 즉시 실패하므로 재시도 로직이 필요하다.
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Kafka에서 스트리밍 읽기
            df = (
                spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
                .option("subscribe", KAFKA_TOPIC)
                .option("startingOffsets", "latest")
                .load()
            )

            # Kafka value를 JSON으로 파싱
            parsed_df = (
                df.selectExpr("CAST(value AS STRING) as json_value")
                .select(from_json(col("json_value"), schema).alias("data"))
                .select("data.*")
            )

            # event_time을 ISO-8601 형식에서 타임스탬프로 변환
            timestamped_df = parsed_df.withColumn(
                "event_time",
                to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSS"),
            )

            # Watermark 적용 (늦게 도착하는 이벤트 처리)
            watermarked_df = timestamped_df.withWatermark(
                "event_time", f"{WATERMARK_SECONDS} seconds"
            )

            # 윈도우 집계: 10초 텀블링 윈도우, user_id별 amount 합계
            windowed_df = (
                watermarked_df
                .groupBy(
                    window(col("event_time"), f"{WINDOW_SECONDS} seconds"),
                    col("user_id"),
                )
                .agg(_sum("amount").alias("total_amount"))
            )

            # 스트리밍 쿼리 시작
            query = (
                windowed_df.writeStream
                .outputMode("append")
                .trigger(processingTime="1 second")
                .foreachBatch(write_to_postgres)
                .start()
            )

            print(f"[INFO] 스트리밍 쿼리 시작 성공 (시도 {attempt}회)")
            query.awaitTermination()
            break  # 정상 종료 시

        except Exception as e:
            error_msg = str(e)
            if "UnknownTopicOrPartition" in error_msg and attempt < MAX_RETRIES:
                print(
                    f"[WARN] 토픽 '{KAFKA_TOPIC}'이 아직 없습니다. "
                    f"{RETRY_INTERVAL}초 후 재시도... ({attempt}/{MAX_RETRIES})"
                )
                time.sleep(RETRY_INTERVAL)
            else:
                print(f"[ERROR] 스트리밍 쿼리 실패: {e}")
                raise

    spark.stop()


if __name__ == "__main__":
    main()
