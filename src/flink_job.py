import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

# ===== 설정 =====
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "payment-log")
FLINK_GROUP_ID = os.environ.get("FLINK_GROUP_ID", "flink-payment-group")

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "streamdb")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")

WATERMARK_SECONDS = int(os.environ.get("WATERMARK_SECONDS", "5"))
WINDOW_SECONDS = int(os.environ.get("WINDOW_SECONDS", "10"))
CHECKPOINT_INTERVAL_SECONDS = int(os.environ.get("CHECKPOINT_INTERVAL_SECONDS", "10"))
CHECKPOINTS_DIR = os.environ.get("CHECKPOINTS_DIR", "file:///tmp/flink-checkpoints")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


def main():
    print("[INFO] PyFlink Job 시작")
    print(
        f"[INFO] Kafka: servers={KAFKA_BOOTSTRAP_SERVERS} topic={KAFKA_TOPIC} "
        f"group={FLINK_GROUP_ID}"
    )
    print(f"[INFO] Postgres JDBC URL: {JDBC_URL}")
    print(
        f"[INFO] Event Time Window={WINDOW_SECONDS}s Watermark={WATERMARK_SECONDS}s "
        f"Checkpoint={CHECKPOINT_INTERVAL_SECONDS}s"
    )

    env = StreamExecutionEnvironment.get_execution_environment()
    # 왜: 장애 복구를 위해 주기적으로 상태 스냅샷을 남긴다.
    env.enable_checkpointing(CHECKPOINT_INTERVAL_SECONDS * 1000)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(
        stream_execution_environment=env,
        environment_settings=settings,
    )

    conf = t_env.get_config().get_configuration()
    # 왜: "요구사항 → 기능 매핑"을 코드에 명시한다.
    # - State Backend: RocksDB
    # - Checkpointing: 10초(기본)
    conf.set_string("state.backend.type", "rocksdb")
    conf.set_string("state.checkpoints.dir", CHECKPOINTS_DIR)
    conf.set_string("execution.checkpointing.interval", f"{CHECKPOINT_INTERVAL_SECONDS} s")
    conf.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    conf.set_string(
        "execution.checkpointing.externalized-checkpoint-retention",
        "RETAIN_ON_CANCELLATION",
    )

    source_ddl = f"""
    CREATE TABLE payment_source (
        user_id STRING,
        amount INT,
        event_time TIMESTAMP(3),
        WATERMARK FOR event_time AS event_time - INTERVAL '{WATERMARK_SECONDS}' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{KAFKA_TOPIC}',
        'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
        'properties.group.id' = '{FLINK_GROUP_ID}',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json',
        'json.timestamp-format.standard' = 'ISO-8601',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    )
    """

    sink_ddl = f"""
    CREATE TABLE flink_results (
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        user_id STRING,
        total_amount INT,
        updated_at TIMESTAMP(3),
        PRIMARY KEY (window_start, window_end, user_id) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = '{JDBC_URL}',
        'table-name' = 'flink_results',
        'username' = '{POSTGRES_USER}',
        'password' = '{POSTGRES_PASSWORD}',
        'driver' = 'org.postgresql.Driver'
    )
    """

    # 왜: 요구사항 1(Event Time 정합성) + 2(Late Data 허용)을 Watermark + Window로 구현한다.
    insert_sql = f"""
    INSERT INTO flink_results
    SELECT
        window_start,
        window_end,
        user_id,
        CAST(SUM(amount) AS INT) AS total_amount,
        CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS updated_at
    FROM TABLE(
        TUMBLE(
            TABLE payment_source,
            DESCRIPTOR(event_time),
            INTERVAL '{WINDOW_SECONDS}' SECOND
        )
    )
    GROUP BY window_start, window_end, user_id
    """

    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)
    print("[INFO] Source/Sink DDL 등록 완료. 스트리밍 집계를 시작합니다...")

    # streaming job이므로 wait()는 정상적으로 블로킹된다.
    t_env.execute_sql(insert_sql).wait()


if __name__ == "__main__":
    main()
