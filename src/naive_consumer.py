import json
import os
import time
from datetime import datetime, timedelta

import psycopg2
from confluent_kafka import Consumer, KafkaError

# ===== 설정 =====
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "payment-log")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "python-consumer-group")
CONSUMER_LABEL = os.environ.get("CONSUMER_LABEL", "python-consumer")
RESULT_TABLE = os.environ.get("RESULT_TABLE", "baseline_results")

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.environ.get("POSTGRES_DB", "streamdb")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")

WINDOW_SECONDS = 10
POLL_TIMEOUT_SECONDS = 1.0
ALLOWED_RESULT_TABLES = {"baseline_results", "naive_results"}


def floor_window_start(now: datetime) -> datetime:
    return now.replace(
        second=(now.second // WINDOW_SECONDS) * WINDOW_SECONDS,
        microsecond=0,
    )


def connect_postgres():
    for attempt in range(1, 31):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
            )
            conn.autocommit = False
            return conn
        except Exception as exc:
            print(f"[WARN] Postgres 연결 실패 ({attempt}/30): {exc}")
            time.sleep(1)
    raise RuntimeError("Postgres 연결 실패: 재시도 한도를 초과했습니다.")


def flush_window(conn, window_start: datetime, window_sums: dict, table_name: str):
    # 왜: 교육용 Python Consumer(Processing Time 방식)의 상태는 메모리(dict)에만 있으므로
    # 10초마다 강제로 DB로 밀어넣어야 그나마 결과가 남는다.
    if not window_sums:
        print(f"[FLUSH] table={table_name} window_start={window_start} rows=0 (비어있는 윈도우)")
        return

    rows = [(window_start, user_id, total_amount) for user_id, total_amount in window_sums.items()]
    query = (
        f"INSERT INTO {table_name} (window_start, user_id, total_amount) "
        "VALUES (%s, %s, %s)"
    )
    with conn.cursor() as cur:
        cur.executemany(query, rows)
    conn.commit()
    print(f"[FLUSH] table={table_name} window_start={window_start} rows={len(rows)}")


def main():
    if RESULT_TABLE not in ALLOWED_RESULT_TABLES:
        raise ValueError(
            f"RESULT_TABLE={RESULT_TABLE} 는 허용되지 않습니다. "
            f"허용값: {sorted(ALLOWED_RESULT_TABLES)}"
        )

    print(f"[INFO] {CONSUMER_LABEL} 시작")
    print(f"[INFO] Kafka: {KAFKA_BOOTSTRAP_SERVERS} / Topic: {KAFKA_TOPIC} / Group: {KAFKA_GROUP_ID}")
    print(
        f"[INFO] Postgres: host={POSTGRES_HOST} port={POSTGRES_PORT} "
        f"db={POSTGRES_DB} user={POSTGRES_USER}"
    )
    print(f"[INFO] Result Table: {RESULT_TABLE}")
    print("[INFO] Processing Time 10초 윈도우 집계 시작\n")

    conn = connect_postgres()

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([KAFKA_TOPIC])

    current_window_start = floor_window_start(datetime.now())
    window_sums = {}

    try:
        while True:
            now = datetime.now()

            # 왜: 이벤트 도착 여부와 무관하게 "처리 시간 기준"으로 10초마다 결과를 닫아야
            # Late Event가 다음 윈도우로 섞이는 Naive 방식의 한계를 의도적으로 드러낼 수 있다.
            while now >= current_window_start + timedelta(seconds=WINDOW_SECONDS):
                flush_window(conn, current_window_start, window_sums, RESULT_TABLE)
                window_sums = {}
                current_window_start = current_window_start + timedelta(seconds=WINDOW_SECONDS)

            msg = consumer.poll(POLL_TIMEOUT_SECONDS)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[WARN] Kafka 메시지 오류: {msg.error()}")
                continue

            try:
                event = json.loads(msg.value().decode("utf-8"))
                user_id = event.get("user_id")
                amount = int(event.get("amount", 0))
                event_time = event.get("event_time")
            except Exception as exc:
                print(f"[WARN] 메시지 파싱 실패: {exc} / raw={msg.value()!r}")
                continue

            if not user_id:
                print(f"[WARN] user_id 누락: {event}")
                continue

            # 왜: Naive 구현의 의도된 결함.
            # event_time은 무시하고 "지금 처리하는 시각"의 윈도우에 누적한다.
            window_sums[user_id] = window_sums.get(user_id, 0) + amount
            print(
                f"[PROC] window_start={current_window_start} user={user_id} amount={amount} "
                f"sum={window_sums[user_id]} event_time={event_time} proc_time={datetime.now().isoformat()}"
            )

            # Late Event 감지 + event_log 기록
            if event_time:
                correct_window = floor_window_start(datetime.fromisoformat(event_time))
                if correct_window != current_window_start:
                    print(
                        f"  └─ [LATE-DETECT] 오배치! "
                        f"event_time({event_time}) → 윈도우 {correct_window} 에 속해야 하지만, "
                        f"현재 윈도우 {current_window_start} 에 집계됨"
                    )
                # 개별 이벤트를 event_log에 기록 (Processing Time vs Event Time 비교용)
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO event_log "
                        "(event_time, proc_time, assigned_window, correct_window, user_id, amount) "
                        "VALUES (%s, %s, %s, %s, %s, %s)",
                        (event_time, datetime.now(), current_window_start, correct_window, user_id, amount),
                    )
                conn.commit()

    except KeyboardInterrupt:
        print("\n[INFO] 사용자 종료 요청(Ctrl+C)")
    finally:
        # 왜: 교육 목적상 종료 시 메모리 상태(window_sums)를 강제 flush하지 않는다.
        # 프로세스가 죽으면 아직 flush되지 않은 집계는 그대로 유실된다.
        consumer.close()
        conn.close()
        print(f"[INFO] {CONSUMER_LABEL} 종료")


if __name__ == "__main__":
    main()
