import os
import json
import time
import random
import signal
import sys
import heapq
from datetime import datetime
from confluent_kafka import Producer

# ===== 설정 =====
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "payment-log")


def env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


# 왜: 수업 1단계에서는 "기본 실시간 처리 가능"을 먼저 보여주기 위해 Chaos를 기본 OFF로 둔다.
CHAOS_ENABLED = env_bool("CHAOS_ENABLED", False)
CHAOS_LATE_RATE = max(0.0, min(1.0, float(os.environ.get("CHAOS_LATE_RATE", "0.2"))))
CHAOS_DELAY_SECONDS = max(0.0, float(os.environ.get("CHAOS_DELAY_SECONDS", "5")))
EVENT_INTERVAL_SECONDS = max(0.1, float(os.environ.get("EVENT_INTERVAL_SECONDS", "1")))

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "payment-producer",
})

# ===== 지연 전송 예약 큐 =====
# heapq(힙큐)는 파이썬 표준 라이브러리의 "최소 힙(min-heap)" 자료구조다.
# 리스트를 힙으로 관리하면서, 항상 가장 작은 값이 [0]번 인덱스에 위치한다.
#
# 여기서는 (전송_예정_시각, event) 튜플을 넣는다.
# 튜플 비교는 첫 번째 원소(전송_예정_시각)부터 하므로,
# scheduled[0]에는 항상 "가장 빨리 보내야 할 이벤트"가 자동으로 올라온다.
#
# 핵심 연산 두 가지:
#   heapq.heappush(scheduled, item)  → 새 예약 추가 (O(log n))
#   heapq.heappop(scheduled)         → 가장 빠른 예약 꺼내기 (O(log n))
scheduled = []


def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] 메시지 전송 실패: {err}")


def send_event(label, event):
    payload = json.dumps(event).encode("utf-8")
    producer.produce(
        topic=KAFKA_TOPIC,
        key=event["user_id"].encode("utf-8"),
        value=payload,
        callback=delivery_report,
    )
    producer.poll(0)
    print(f"{label} 전송됨: {json.dumps(event, ensure_ascii=False)}")


def flush_scheduled():
    """종료 전 예약 큐에 남은 이벤트를 모두 즉시 전송"""
    while scheduled:
        _send_time, event = heapq.heappop(scheduled)
        send_event("[LATE-FLUSH]", event)


def signal_handler(sig, frame):
    print("\n[INFO] 종료 신호를 받았습니다. Producer를 안전하게 종료합니다...")
    flush_scheduled()
    producer.flush()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ===== 메인 루프 =====
print(f"[INFO] Kafka Producer 시작 | Servers: {KAFKA_BOOTSTRAP_SERVERS} | Topic: {KAFKA_TOPIC}")
if CHAOS_ENABLED:
    print(
        f"[INFO] Chaos ON | late_rate={CHAOS_LATE_RATE:.2f} | "
        f"delay={CHAOS_DELAY_SECONDS:.1f}s"
    )
else:
    print("[INFO] Chaos OFF | 모든 이벤트를 즉시 전송")
print(f"[INFO] 이벤트 생성 시작... interval={EVENT_INTERVAL_SECONDS:.1f}s (Ctrl+C로 종료)\n")

while True:
    now = time.time()

    # 1. 예약 큐에서 전송 시각이 된 이벤트를 꺼내서 전송
    #    scheduled[0]은 힙의 최솟값 = 가장 먼저 보내야 할 (send_time, event) 튜플이다.
    #    peek(꺼내지 않고 확인)으로 send_time만 비교한 뒤, 시간이 됐으면 pop으로 꺼낸다.
    while scheduled:
        earliest_send_time, _earliest_event = scheduled[0]  # peek: 꺼내지 않고 확인만
        if earliest_send_time > now:
            break  # 아직 전송할 시각이 안 됐으면 루프 탈출
        _send_time, event = heapq.heappop(scheduled)        # pop: 실제로 큐에서 제거
        send_event("[LATE]", event)

    # 2. 새 이벤트 생성
    event = {
        "user_id": f"U{random.randint(1, 5)}",
        "amount": random.randint(500, 5000),
        # 왜: Flink Watermark time column은 TIMESTAMP(3)까지 지원하므로 밀리초 정밀도로 맞춘다.
        "event_time": datetime.now().isoformat(timespec="milliseconds"),
    }

    # 3. Chaos ON일 때만 지연 예약을 주입한다.
    # 왜: 1단계(가능성 확인)와 2단계(한계 노출)를 같은 코드로 전환하기 위함.
    if CHAOS_ENABLED and random.random() < CHAOS_LATE_RATE:
        send_time = time.time() + CHAOS_DELAY_SECONDS
        # heappush: 힙에 삽입하면 자동으로 send_time 기준 정렬이 유지된다.
        heapq.heappush(scheduled, (send_time, event))
        print(
            f"[LATE] 예약됨: {json.dumps(event, ensure_ascii=False)} "
            f"→ {CHAOS_DELAY_SECONDS:.1f}초 후 전송"
        )
    else:
        send_event("[NORMAL]", event)

    # 4. 1초 대기
    time.sleep(EVENT_INTERVAL_SECONDS)
