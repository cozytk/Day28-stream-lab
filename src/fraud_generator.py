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
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "fraud-payments")
# 왜: 사기 패턴 주입 비율을 환경변수로 제어 → 수업 중 비율 조절 시연 가능
FRAUD_RATE = float(os.environ.get("FRAUD_RATE", "0.1"))
EVENT_INTERVAL_SECONDS = max(0.1, float(os.environ.get("EVENT_INTERVAL_SECONDS", "1")))

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "fraud-payment-producer",
})

# ===== 지연 전송 예약 큐 =====
# 왜: 사기 패턴의 "소액 → (시간차) → 고액" 순서를 구현하기 위해
# heapq로 고액 결제를 미래 시점에 예약한다.
# producer.py와 동일한 패턴이지만, 여기서는 Chaos가 아니라 "사기 시뮬레이션"이 목적이다.
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
        send_event("[FLUSH]", event)


def signal_handler(sig, frame):
    print("\n[INFO] 종료 신호를 받았습니다. Fraud Generator를 안전하게 종료합니다...")
    flush_scheduled()
    producer.flush()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ===== 메인 루프 =====
print(f"[INFO] Fraud Generator 시작 | Servers: {KAFKA_BOOTSTRAP_SERVERS} | Topic: {KAFKA_TOPIC}")
print(f"[INFO] FRAUD_RATE={FRAUD_RATE:.2f} | interval={EVENT_INTERVAL_SECONDS:.1f}s")
print("[INFO] 사기 패턴: 소액(<1000) 결제 후 1분 이내 고액(>10000) 결제")
print("[INFO] 이벤트 생성 시작... (Ctrl+C로 종료)\n")

while True:
    now = time.time()

    # 1. 예약 큐에서 전송 시각이 된 이벤트를 꺼내서 전송
    #    왜: 사기 패턴의 "고액 결제"는 소액 결제 후 5~30초 뒤에 예약되어 있다.
    #    heapq 덕분에 scheduled[0]이 항상 가장 빨리 보내야 할 이벤트이다.
    while scheduled:
        earliest_send_time, _earliest_event = scheduled[0]  # peek
        if earliest_send_time > now:
            break
        _send_time, event = heapq.heappop(scheduled)  # pop
        # 왜: 예약 시점에는 event_time을 알 수 없으므로, 실제 전송 직전에 현재 시각을 찍는다.
        event["event_time"] = datetime.now().isoformat(timespec="milliseconds")
        send_event("[FRAUD-LARGE]", event)

    # 2. 새 이벤트 생성
    user_id = f"U{random.randint(1, 5)}"
    event_time = datetime.now().isoformat(timespec="milliseconds")

    # 3. 사기 패턴 주입 여부 결정
    if random.random() < FRAUD_RATE:
        # ──────────────────────────────────────────────────────
        # 사기 패턴 (Fraud Pattern)
        # 왜: 실제 카드 사기의 전형적 패턴을 시뮬레이션한다.
        #   1단계: 소액 결제로 카드 유효성 확인 (100~900원)
        #   2단계: 유효성 확인 후 고액 결제 시도 (15000~50000원)
        #   시간차: 5~30초 (실제로는 수 분~수 시간이지만 수업 시연용으로 단축)
        #
        # FraudDetector(flink_fraud.py)가 이 패턴을 탐지해야 한다:
        #   "같은 user_id에서 소액(<1000) 후 1분 이내 고액(>10000) 발생"
        # ──────────────────────────────────────────────────────
        small_amount = random.randint(100, 900)
        small_event = {
            "user_id": user_id,
            "amount": small_amount,
            "event_time": event_time,
        }
        send_event("[FRAUD-SMALL]", small_event)

        # 고액 결제를 5~30초 뒤로 예약
        delay = random.uniform(5, 30)
        large_amount = random.randint(15000, 50000)
        large_event = {
            "user_id": user_id,  # 왜: 반드시 같은 user_id여야 사기 패턴으로 탐지됨
            "amount": large_amount,
            "event_time": "",  # 왜: 실제 전송 시점에 갱신 (아래 주석 참고)
        }
        # 왜: event_time은 전송 시점에 찍어야 Flink에서 처리 시각과 일치한다.
        # 하지만 heapq에 넣을 때는 아직 전송 시점을 모르므로, 빈 문자열로 두고
        # 꺼낼 때 갱신하는 방식을 쓴다. → 아래 flush 로직에서 처리.
        # 단순화: 예약 시점에 미리 event_time을 찍지 않고, 전송 직전에 갱신.
        send_time = time.time() + delay
        heapq.heappush(scheduled, (send_time, large_event))
        print(
            f"  └─ [FRAUD] {user_id}: 소액({small_amount}원) 전송 완료, "
            f"고액({large_amount}원) {delay:.1f}초 후 예약됨"
        )
    else:
        # ──────────────────────────────────────────────────────
        # 정상 결제 (Normal Payment)
        # 왜: 대부분의 결제는 정상이어야 사기 탐지의 의미가 있다.
        # 금액 범위(500~5000)는 producer.py와 동일.
        # ──────────────────────────────────────────────────────
        normal_amount = random.randint(500, 5000)
        normal_event = {
            "user_id": user_id,
            "amount": normal_amount,
            "event_time": event_time,
        }
        send_event("[NORMAL]", normal_event)

    # 4. 대기
    time.sleep(EVENT_INTERVAL_SECONDS)
