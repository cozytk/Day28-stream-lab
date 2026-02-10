# Stream Lab: 실시간 데이터 처리 실습

## 빠른 실행 (Justfile)

이 실습은 `just` 커맨드로 Phase별 환경을 간편하게 관리할 수 있습니다.

**왜 Justfile을 사용하나요?**

실습 중 서비스를 중단/재시작하거나 Phase를 전환할 때 다음과 같은 문제가 자주 발생합니다:

- **Flink Job 중복**: 재시작할 때마다 기존 Job을 취소하지 않으면 여러 Job이 동시에 돌아감
- **DB 잔여 데이터**: 이전 Phase의 데이터가 남아 결과가 섞임
- **Kafka 토픽 미생성**: Flink Job을 먼저 제출하면 토픽이 없어서 재시작 루프에 빠짐
- **컨테이너 재생성**: 환경 변수를 바꿔서 `docker compose up`하면 의존 서비스까지 재생성됨

Justfile은 이런 문제를 모두 처리하여, 각 Phase를 깔끔한 상태에서 시작할 수 있게 해줍니다.

### 설치

```bash
# Ubuntu/WSL
curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to /usr/local/bin

# macOS
brew install just
```

### 주요 커맨드

```bash
just              # 사용 가능한 레시피 목록 보기

# === Phase별 실행 ===
just phase1       # Phase 1: 전체 초기화 → Chaos OFF → baseline_results
just phase2       # Phase 2: Chaos ON → naive_results (Phase 1 이후 실행)
just phase4       # Phase 4: 전체 초기화 → Flink + Chaos ON 비교

# === 결과 조회 ===
just query-phase1 # Phase 1 집계 결과
just query-phase2 # Phase 2 Processing Time vs Event Time 비교
just query-phase4 # Phase 4 Flink vs 정답 vs Python 비교
just late-events  # Late Event 발생/감지 로그

# === Phase 전환 ===
just phase1-done  # Phase 1 → 2 전환 (Generator/Consumer만 중지)
just phase2-done  # Phase 2 → 4 전환 (Generator/Consumer만 중지)

# === 유틸리티 ===
just status             # 컨테이너 + Flink Job 상태 확인
just cancel-flink-jobs  # 실행 중인 모든 Flink Job 취소
just reset-db           # DB 테이블 전체 초기화
just logs generator     # 특정 서비스 로그 확인
just clean              # 모든 컨테이너 + 볼륨 완전 삭제
```

### 전체 실습 흐름 (권장)

```bash
just phase1           # Phase 1 시작
# ... 1분 후 ...
just query-phase1     # 결과 확인
just phase1-done      # 정리

just phase2           # Phase 2 시작
# ... 1분 후 ...
just query-phase2     # 결과 확인
just late-events      # Late Event 로그 확인
just phase2-done      # 정리

# Phase 3은 이론 학습 (실행 없음)

just phase4           # Phase 4 시작 (전체 초기화 포함)
# ... 2분 후 ...
just query-phase4     # Flink vs Python 비교
just clean            # 실습 종료 후 정리
```

> **참고**: `just`가 설치되어 있지 않아도 각 Phase의 `docker compose` 명령어를 직접 실행할 수 있습니다. 아래 Phase별 섹션을 참고하세요.

---

## 학습 목표

이 실습을 통해 다음을 배웁니다:

1. **Python만으로 실시간 데이터 처리가 가능하다는 것을 확인**
2. **운영 환경에서 Python 구현의 한계 실감** (Late Event, Processing Time 문제, 상태 유실)
3. **Apache Flink가 이런 문제를 어떻게 해결하는지 이해** (Event Time, Watermark, Checkpointing, Upsert)

## 사전 준비

## 아키텍처

![Stream Lab Architecture](https://cdn.discordapp.com/attachments/1457516082071081045/1470232242319261860/image.png?ex=698a8bfa&is=69893a7a&hm=736cf40bba8eac893bc99d251a8c3c45a49c4a06ac62f1166dffe3d6f134212d&)

```
┌─────────────────────────────────────────────────────────────────┐
│                         Stream Lab                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  [Generator]                                                    │
│  (producer.py)                                                  │
│      │                                                          │
│      │ Payment Events                                           │
│      │ {user_id, amount, event_time}                           │
│      │                                                          │
│      ▼                                                          │
│  ┌──────────┐                                                   │
│  │  Kafka   │                                                   │
│  │ (KRaft)  │  Topic: payment-log                              │
│  └────┬─────┘                                                   │
│       │                                                         │
│       ├──────────────────┬─────────────────────────┐           │
│       │                  │                         │           │
│       ▼                  ▼                         ▼           │
│  [Python Consumer]  [Python Consumer]    [Flink Cluster]      │
│  (naive_consumer)   (naive_consumer)     (jobmanager +        │
│       │                  │                taskmanager)         │
│  Processing Time    Processing Time           │               │
│  (10s window)       (10s window)          Event Time          │
│       │                  │              (10s TUMBLE)           │
│       │                  │              + Watermark (5s)       │
│       │                  │                   │                 │
│       ▼                  ▼                   ▼                 │
│  ┌─────────────────────────────────────────────────┐          │
│  │             PostgreSQL (streamdb)                │          │
│  ├─────────────────────────────────────────────────┤          │
│  │  baseline_results  │  naive_results  │  flink_results │   │
│  │  (Phase 1)         │  (Phase 2)      │  (Phase 4)     │   │
│  └─────────────────────────────────────────────────┘          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 실습 시나리오

### 배경 스토리
여러분은 결제 시스템의 실시간 집계를 담당하는 데이터 엔지니어입니다.
**사용자별 10초 단위 결제 금액 합계**를 실시간으로 계산하여 DB에 저장해야 합니다.

### 데이터 흐름
1. **Generator**: 결제 이벤트 생성 → Kafka 전송
2. **Consumer**: Kafka 구독 → 10초 윈도우로 집계 → PostgreSQL 저장
3. **모니터링**: PostgreSQL 쿼리로 결과 비교

---

## Phase 1: "Python으로도 실시간 처리가 된다"

### 목표
정상적인 환경(데이터 지연 없음)에서 Python만으로 실시간 집계가 가능함을 확인합니다.

### 실행 단계

#### 1. 인프라 시작
```bash
cd stream-lab   # 실습 디렉토리로 이동

# Kafka와 PostgreSQL 시작
docker compose up -d kafka postgres

# 서비스 준비 대기
docker compose logs -f kafka
# "Kafka Server started" 메시지 확인 후 Ctrl+C
```

> **참고**: `compose.yml`의 Kafka 환경 변수 설정이 복잡해 보일 수 있으나, 대부분 [Apache Kafka Docker Hub](https://hub.docker.com/r/apache/kafka) 공식 문서에서 제공하는 KRaft 모드 기본 설정입니다.


#### 2. Generator와 Python Consumer 시작 (Chaos OFF)
```bash
# Generator와 Python Consumer 동시 시작 (기본값: CHAOS_ENABLED=false, RESULT_TABLE=baseline_results)
docker compose up -d generator python-consumer

# 로그 확인
docker compose logs -f generator python-consumer
```

#### 3. 결과 확인 (1분 후)
```bash
# PostgreSQL 접속
docker compose exec postgres psql -U postgres -d streamdb

# 집계 결과 조회
SELECT
    window_start,
    user_id,
    total_amount,
    updated_at
FROM baseline_results
ORDER BY window_start, user_id
LIMIT 20;

# 윈도우별 총 금액 확인
SELECT
    window_start,
    COUNT(*) as user_count,
    SUM(total_amount) as total
FROM baseline_results
GROUP BY window_start
ORDER BY window_start;

# 종료: \q
```

### 예상 결과
```
    window_start     | user_id | total_amount |         updated_at
---------------------+---------+--------------+----------------------------
 2026-02-09 00:00:00 | U1      |         3842 | 2026-02-09 00:00:10.123456
 2026-02-09 00:00:00 | U2      |         4521 | 2026-02-09 00:00:10.123456
 2026-02-09 00:00:10 | U1      |         2156 | 2026-02-09 00:00:20.234567
```

### 관찰 포인트
- **10초마다 윈도우가 닫히면서 집계 결과가 INSERT됨**
- **각 윈도우 내 사용자별 결제 금액 합계가 정확함**
- **updated_at이 윈도우 종료 직후 시간임 (Processing Time)**

### 코드로 이해하기

이 결과가 어떻게 만들어지는지 핵심 코드를 살펴보겠습니다.

**이벤트 생성 (`src/producer.py`)**

Generator는 매초 결제 이벤트를 생성하여 Kafka로 전송합니다:

```python
# src/producer.py:107-112
event = {
    "user_id": f"U{random.randint(1, 5)}",
    "amount": random.randint(500, 5000),
    "event_time": datetime.now().isoformat(timespec="milliseconds"),
}
```

- `user_id`: U1~U5 중 랜덤 선택
- `amount`: 500~5000원 랜덤 금액
- `event_time`: 이벤트 **생성 시점**의 ISO-8601 타임스탬프 (예: `2026-02-09T01:23:45.678`)

**Processing Time 윈도우 집계 (`src/naive_consumer.py`)**

Consumer는 `datetime.now()` 기준으로 10초 윈도우를 관리합니다:

```python
# src/naive_consumer.py:27-31
def floor_window_start(now: datetime) -> datetime:
    """현재 시각을 10초 단위로 내림 (예: 01:23:47 → 01:23:40)"""
    return now.replace(
        second=(now.second // WINDOW_SECONDS) * WINDOW_SECONDS,
        microsecond=0,
    )
```

```python
# src/naive_consumer.py:98-99
current_window_start = floor_window_start(datetime.now())
window_sums = {}   # {user_id: total_amount} - 메모리에만 존재!
```

10초가 지나면 윈도우를 닫고 DB에 INSERT합니다:

```python
# src/naive_consumer.py:107-110
while now >= current_window_start + timedelta(seconds=WINDOW_SECONDS):
    flush_window(conn, current_window_start, window_sums, RESULT_TABLE)
    window_sums = {}
    current_window_start = current_window_start + timedelta(seconds=WINDOW_SECONDS)
```

Chaos OFF 상태에서는 이벤트가 즉시 도착하므로, **Processing Time ≈ Event Time**이 되어 정확한 결과가 나옵니다.

### Phase 1 정리
```bash
# 다음 Phase를 위해 중지
docker compose stop generator python-consumer
```

**배운 것**: Python + Kafka + PostgreSQL만으로도 실시간 집계가 가능하다!

---

## Phase 2: "그런데 데이터가 늦게 오면?"

### 목표
운영 환경에서 발생하는 **Late Event**(늦게 도착하는 이벤트) 상황을 재현하고, Python 구현의 한계를 체감합니다.

### 왜 데이터가 늦게 도착할까요? (Late Event의 원인)

실제 운영 환경에서는 이벤트가 발생한 시각(**Event Time**)과 서버에 도착한 시각(**Processing Time**) 사이에 차이가 발생합니다.

*   **네트워크 불안정**: 사용자가 지하철을 타고 가다 터널에 진입해 인터넷이 잠시 끊겼다고 가정해 봅시다. 결제는 터널 안에서 일어났지만(Event Time), 데이터는 터널을 빠져나와 네트워크가 복구된 5초 뒤에야 서버에 전달됩니다(Processing Time).
*   **기기 내 버퍼링**: 모바일 앱은 배터리 효율을 위해 이벤트를 발생 즉시 보내지 않고 일정량 모아서 보내는 경우(Batching)가 많습니다. 예를 들어, 사용자가 결제 버튼을 눌렀을 때는 즉시 발생하지만, 데이터는 5초 후에 서버에 전달됩니다.
*   **시스템 부하**: 서버나 데이터베이스에 갑자기 많은 요청이 몰려 처리가 지연되면, 뒤따르는 이벤트들이 줄줄이 밀리면서 실제 발생 시각보다 늦게 큐에 쌓이게 됩니다.
*   **분산 환경의 특성**: 여러 서버에서 데이터를 전송할 때, 네트워크 경로 차이로 인해 먼저 발생한 이벤트가 나중에 도착하기도 합니다.



### Chaos Engineering 활성화
Generator에 `CHAOS_ENABLED=true` 설정 시:
- 전체 이벤트의 **20%가 5초 지연**되어 발송됨
- 예: `event_time=00:00:03`인 이벤트가 `00:00:08`에 도착

실제 구현을 보면, Generator는 `heapq`(최소 힙)를 사용하여 Late Event를 구현합니다:

```python
# src/producer.py:116-123
if CHAOS_ENABLED and random.random() < CHAOS_LATE_RATE:
    send_time = time.time() + CHAOS_DELAY_SECONDS
    heapq.heappush(scheduled, (send_time, event))
    print(
        f"[LATE] 예약됨: {json.dumps(event, ensure_ascii=False)} "
        f"→ {CHAOS_DELAY_SECONDS:.1f}초 후 전송"
    )
else:
    send_event("[NORMAL]", event)
```

메인 루프 상단에서 예약 시간이 된 이벤트를 꺼내 전송합니다:

```python
# src/producer.py:99-104
while scheduled:
    earliest_send_time, _earliest_event = scheduled[0]  # peek: 꺼내지 않고 확인만
    if earliest_send_time > now:
        break                                            # 아직 전송 시각이 안 됐으면 탈출
    _send_time, event = heapq.heappop(scheduled)         # pop: 실제로 큐에서 제거
    send_event("[LATE]", event)
```

핵심: Late Event의 `event_time`은 **생성 시점**(5초 전)이지만, Kafka에 **도착하는 시점**은 현재입니다.

### 실행 단계

#### 1. Chaos 모드로 재시작
```bash
# Chaos 모드로 Generator + Consumer 재시작
CHAOS_ENABLED=true RESULT_TABLE=naive_results KAFKA_GROUP_ID=python-consumer-limit \
  docker compose up -d generator python-consumer

# 로그에서 Late Event 발생 확인
docker compose logs -f generator | grep LATE
```

#### 2. Late Event 오배치 확인

Consumer가 Late Event를 자동으로 감지합니다:

```bash
# Late Event가 잘못된 윈도우에 배치된 로그만 필터링
docker compose logs python-consumer | grep "LATE-DETECT"
```

#### 3. 핵심 비교: "총합계는 같은데 윈도우 분포가 다르다"

Consumer는 모든 이벤트를 `event_log` 테이블에 기록합니다.
각 이벤트의 **실제 배치 윈도우**(Processing Time)와 **올바른 윈도우**(Event Time)를 비교할 수 있습니다:

```bash
docker compose exec postgres psql -U postgres -d streamdb
```

```sql
-- 1) 전체 합계 비교: 동일해야 함 (같은 이벤트이므로)
SELECT
    'Processing Time 기준' AS method, SUM(amount) AS grand_total
FROM event_log
UNION ALL
SELECT
    'Event Time 기준', SUM(amount)
FROM event_log;

-- 2) 윈도우별 비교: 여기서 차이가 드러남!
SELECT
    COALESCE(p.window_start, e.window_start) AS window_start,
    COALESCE(p.proc_total, 0) AS proc_total,
    COALESCE(e.event_total, 0) AS event_total,
    COALESCE(p.proc_total, 0) - COALESCE(e.event_total, 0) AS diff
FROM (
    SELECT assigned_window AS window_start, SUM(amount) AS proc_total
    FROM event_log GROUP BY assigned_window
) p
FULL OUTER JOIN (
    SELECT correct_window AS window_start, SUM(amount) AS event_total
    FROM event_log GROUP BY correct_window
) e ON p.window_start = e.window_start
ORDER BY window_start;
```

### 예상 결과

**1) 전체 합계**: 두 방식 모두 동일합니다 (같은 이벤트를 집계했으므로).

```
        method        | grand_total
----------------------+-------------
 Processing Time 기준 |      216567
 Event Time 기준      |      216567   ← 동일!
```

**2) 윈도우별 비교**: Late Event가 있는 윈도우에서 `diff ≠ 0`이 나타납니다:

```
    window_start     | proc_total | event_total |  diff
---------------------+------------+-------------+--------
 2026-02-09 03:33:30 |      32522 |       28603 |   3919  ← Late Event 유입
 2026-02-09 03:33:40 |      25518 |       21606 |   3912  ← Late Event 유입
 2026-02-09 03:33:50 |      22669 |       28931 |  -6262  ← Late Event 빠짐
 2026-02-09 03:34:00 |      28109 |       28554 |   -445  ← Late Event 빠짐
 2026-02-09 03:34:10 |      27589 |       28136 |   -547  ← Late Event 빠짐
```

- `diff > 0`: Late Event가 **잘못 유입**된 윈도우 (과대 집계)
- `diff < 0`: Late Event가 **빠져나간** 윈도우 (과소 집계)
- `diff = 0`: Late Event의 영향이 없는 윈도우

**왜 이런 일이 발생할까?**

```
Timeline:
02:01:07 — event_time인 결제 발생 (amount=700)
            → Chaos에 의해 5초 지연 예약됨 (Kafka에 아직 전송 안 됨)
02:01:10 — Consumer가 윈도우(02:01:00~02:01:10)를 닫고 DB에 flush
            → amount=700은 아직 Kafka에 없으므로 이 윈도우에서 누락
02:01:12 — Producer가 지연된 이벤트를 Kafka에 전송
            → Consumer가 수신하여 현재 윈도우(02:01:10~02:01:20)에 집계

Result: 윈도우 02:01:00~02:01:10 → 700 누락 (과소 집계)
        윈도우 02:01:10~02:01:20 → 700 추가 (과대 집계)
```

실제 코드에서 이 문제가 발생하는 지점을 확인해보세요:

```python
# src/naive_consumer.py:135-141
# 의도된 결함: event_time은 무시하고 "지금 처리하는 시각"의 윈도우에 누적한다.
window_sums[user_id] = window_sums.get(user_id, 0) + amount
print(
    f"[PROC] window_start={current_window_start} user={user_id} amount={amount} "
    f"sum={window_sums[user_id]} event_time={event_time} proc_time={datetime.now().isoformat()}"
)
```

`event_time`을 로그에 **출력은 하지만 윈도우 분류에 사용하지 않습니다**. 어떤 이벤트든 `current_window_start`(Processing Time 기준) 윈도우에 무조건 합산됩니다.

또한, 프로세스 종료 시 아직 닫히지 않은 윈도우의 데이터가 유실됩니다:

```python
# src/naive_consumer.py:146-150
finally:
    # 교육 목적상 종료 시 메모리 상태(window_sums)를 강제 flush하지 않는다.
    # 프로세스가 죽으면 아직 flush되지 않은 집계는 그대로 유실된다.
    consumer.close()
    conn.close()
```

### 관찰 포인트
- **동일한 event_time이라도 도착 시간(Processing Time)이 다르면 다른 윈도우로 분류됨**
- **Late Event는 원래 속한 윈도우가 이미 닫혀서 잘못된 윈도우에 집계됨**
- **총합계도 틀리지는 않지만, 윈도우별 분포가 부정확함**

### Phase 2 정리
```bash
docker compose stop generator python-consumer
```

**문제 인식**: 실시간 환경에서는 네트워크 지연, 시스템 지연으로 Late Event가 필연적으로 발생한다. Processing Time만으로는 정확한 집계가 불가능하다.

---

## Phase 3: "그러면 뭐가 필요할까?"

### 생각해보기

Phase 2의 문제를 해결하려면 무엇이 필요할까요? 다음 질문들을 생각해보세요.

#### 1. 시간 기준
**Q**: Late Event 문제를 해결하려면 어떤 시간을 기준으로 윈도우를 나눠야 할까?

<details>
<summary>힌트</summary>

- Processing Time(처리 시간): Consumer가 메시지를 받은 시간 (Python `datetime.now()`)
- Event Time(이벤트 시간): 실제 결제가 발생한 시간 (데이터 내부의 `event_time` 필드)

어느 것이 "진실"인가?
</details>

#### 2. 늦은 데이터 대기
**Q**: Event Time 기준으로 윈도우를 나눈다면, 언제까지 Late Event를 기다려야 할까?

<details>
<summary>힌트</summary>

- 무한정 기다릴 수 없음 (윈도우를 닫아야 결과 출력 가능)
- "5초까지는 기다리자" 같은 기준이 필요 → **Watermark** 개념
</details>

#### 3. 상태 관리
**Q**: Python Consumer가 재시작되면 집계 중인 데이터가 사라진다. 어떻게 해결할까?

<details>
<summary>힌트</summary>

현재 Python 구현은 메모리(dict)에만 상태를 보관합니다:

```python
# src/naive_consumer.py:99
window_sums = {}   # {user_id: total_amount} - 메모리에만 존재!
```

프로세스가 종료되면 이 dict는 사라지고, 아직 flush되지 않은 윈도우 집계도 함께 유실됩니다.

해결책:
- 디스크에 상태 저장 (예: RocksDB)
    - **RocksDB**: Meta(Facebook)에서 개발한 고성능 임베디드 Key-Value 저장소로, SSD 환경에 최적화되어 빠른 읽기/쓰기를 지원하며 Flink나 Kafka Streams의 기본 상태 저장소로 자주 활용됨.
- 주기적으로 체크포인트 생성 → 재시작 시 복구
</details>

#### 4. 결과 업데이트
**Q**: Late Event가 도착했을 때, 이미 DB에 저장된 윈도우 결과를 어떻게 수정할까?

<details>
<summary>힌트</summary>

현재 Python 구현의 DB 쓰기 방식:

```python
# src/naive_consumer.py:60-66
query = (
    f"INSERT INTO {table_name} (window_start, user_id, total_amount) "
    "VALUES (%s, %s, %s)"
)
with conn.cursor() as cur:
    cur.executemany(query, rows)
```

→ 항상 `INSERT`만 수행. 같은 윈도우/유저 결과가 다시 생성되면 중복 행이 발생합니다.

DB 스키마를 비교해보면 차이가 명확합니다:

```sql
-- sql/init.sql: baseline/naive 테이블에는 PK가 없다
CREATE TABLE baseline_results (
    window_start TIMESTAMP,
    user_id      VARCHAR(50),
    total_amount INT,
    updated_at   TIMESTAMP DEFAULT NOW()
);

-- flink_results 테이블에는 PK가 있다 → UPSERT 가능
CREATE TABLE flink_results (
    ...
    PRIMARY KEY (window_start, window_end, user_id)
);
```

필요한 것:
- **UPSERT** (있으면 UPDATE, 없으면 INSERT)
- Primary Key 설정 (window_start, window_end, user_id)
</details>

### 요구사항 정리

**생각해보기**를 통해 도출된 요구사항:

| 요구사항 | Python 구현 | 필요한 기능 |
|---------|------------|------------|
| **정확한 윈도우 분류** | Processing Time (부정확) | Event Time 기반 처리 |
| **Late Event 대응** | 윈도우 닫히면 끝 | Watermark (지연 허용) |
| **상태 복구** | dict (휘발성) | RocksDB + Checkpointing |
| **결과 수정** | INSERT만 가능 | UPSERT (Primary Key 기반) |

**결론**: 이런 기능들을 직접 구현하는 것은 매우 복잡하다. → **Apache Flink 같은 전문 스트림 처리 프레임워크가 필요!**

---

## Phase 4: "Flink는 이걸 어떻게 해결하는가"

### 목표
PyFlink로 동일한 집계를 구현하고, Chaos 모드에서도 정확한 결과를 내는 것을 확인합니다.

### Flink 아키텍처

```
┌────────────────────────────────────────────────────────────┐
│                    Flink Cluster                           │
├────────────────────────────────────────────────────────────┤
│                                                            │
│  [JobManager]                 [TaskManager]                │
│  - Job 관리                   - Task 실행                   │
│  - Checkpoint 조율            - RocksDB 상태 저장          │
│  - Web UI (8081)              - Kafka Consumer             │
│                               - Window 집계                 │
│                               - PostgreSQL Writer          │
│                                                            │
│  Checkpoint Storage: /tmp/flink-checkpoints (컨테이너 내부) │
│  State Backend: RocksDB (디스크 기반, 복구 가능)            │
└────────────────────────────────────────────────────────────┘
```

### 실행 단계

> **`just phase4` 한 줄이면 아래 1~4단계를 자동으로 수행합니다.**
> 수동으로 실행하려면 아래 순서를 따르세요.

#### 1. Flink 클러스터 시작
```bash
# 기존 서비스 중지 & 테이블 초기화
docker compose stop generator python-consumer
docker compose exec postgres psql -U postgres -d streamdb \
  -c "TRUNCATE naive_results; TRUNCATE event_log;"

# JobManager + TaskManager 시작
docker compose up -d jobmanager taskmanager

# Web UI 접속 (브라우저)
# http://localhost:8081

# 클러스터 상태 확인
docker compose logs jobmanager | grep "started"
```

#### 2. Kafka 토픽 생성 & Flink Job 제출
```bash
# Kafka 토픽 미리 생성 (Flink Job이 토픽 없음 에러를 내지 않도록)
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic payment-log \
  --partitions 1 --replication-factor 1 \
  --if-not-exists

# Flink 클러스터 준비 대기 (약 40초)
docker compose exec jobmanager flink list   # "No running jobs." 확인

# PyFlink Job 실행 (Generator보다 먼저! 첫 이벤트부터 수신)
docker compose exec jobmanager flink run -py /opt/flink/src/flink_job.py

# Job 실행 확인 (Web UI에서 Running Jobs 확인 또는)
docker compose exec jobmanager flink list
```

> **주의: Flink Job을 Generator보다 먼저 제출해야 합니다.**
> Flink의 Kafka Source는 `scan.startup.mode = latest-offset`로 설정되어 있어,
> Job 제출 시점 이후에 Kafka에 들어온 이벤트만 읽습니다.
> Generator가 먼저 시작되면 그 사이 생산된 이벤트를 Flink가 놓쳐서
> 첫 몇 개 윈도우에서 Flink 결과가 정답보다 적게 나옵니다.

#### 3. Chaos 모드로 Generator + Consumer 동시 시작
```bash
# Generator + Consumer를 동시에 시작 (같은 시점부터 이벤트 생산/소비)
CHAOS_ENABLED=true RESULT_TABLE=naive_results KAFKA_GROUP_ID=python-consumer-flink \
  docker compose up -d generator python-consumer

# Late Event 발생 확인
docker compose logs -f generator | grep LATE

# TaskManager 로그 확인
docker compose logs -f taskmanager
```

#### 4. 실시간 처리 확인
```bash
# PostgreSQL에서 Flink 결과 조회 (1분 후부터)
docker compose exec postgres psql -U postgres -d streamdb

-- Flink 결과 확인
SELECT
    window_start,
    window_end,
    user_id,
    total_amount,
    updated_at
FROM flink_results
ORDER BY window_start, user_id
LIMIT 20;

-- 특정 윈도우의 업데이트 횟수 확인 (UPSERT 동작)
SELECT
    window_start,
    user_id,
    total_amount,
    updated_at
FROM flink_results
WHERE window_start = (SELECT MIN(window_start) FROM flink_results)
ORDER BY user_id;
```

#### 5. 최종 비교: Flink vs Python (3분 실행 후)

Phase 4에서는 Generator + Python Consumer + Flink가 **같은 Kafka 이벤트**를 동시에 처리합니다.
`event_log` 테이블에 기록된 개별 이벤트의 "올바른 윈도우"와 Flink 결과를 비교할 수 있습니다:

```bash
docker compose exec postgres psql -U postgres -d streamdb

-- 1) 전체 합계: Flink는 Watermark 지연으로 마지막 윈도우가 미방출되어 약간 적을 수 있음
SELECT 'Flink (Event Time)' AS method, SUM(total_amount) AS grand_total FROM flink_results
UNION ALL
SELECT '정답 (Event Time)', SUM(amount) FROM event_log;

-- 2) 윈도우별 비교: Flink는 정답에 가깝고, Python은 틀림
SELECT
    COALESCE(f.window_start, e.window_start) AS window_start,
    COALESCE(f.flink_total, 0)  AS "Flink",
    COALESCE(e.event_total, 0)  AS "정답(Event Time)",
    COALESCE(p.proc_total, 0)   AS "Python(Proc Time)"
FROM (
    SELECT window_start, SUM(total_amount) AS flink_total
    FROM flink_results GROUP BY window_start
) f
FULL OUTER JOIN (
    SELECT correct_window AS window_start, SUM(amount) AS event_total
    FROM event_log GROUP BY correct_window
) e ON f.window_start = e.window_start
FULL OUTER JOIN (
    SELECT assigned_window AS window_start, SUM(amount) AS proc_total
    FROM event_log GROUP BY assigned_window
) p ON COALESCE(f.window_start, e.window_start) = p.window_start
ORDER BY window_start;
```

### 예상 결과

**1) 전체 합계**: Flink와 event_log의 합계가 근사합니다. 차이가 나는 이유는 아래 참고.

**2) 윈도우별 비교**: 안정 구간에서 **Flink = 정답**, Python은 다릅니다:
```
    window_start     | flink  | correct | naive
---------------------+--------+---------+--------
 2026-02-09 03:51:00 |  34000 |   34000 |  38485  ← naive 과대
 2026-02-09 03:51:10 |  29033 |   29033 |  24225  ← naive 과소
 2026-02-09 03:51:20 |  26421 |   26421 |  31229  ← naive 과대
 2026-02-09 03:52:00 |  27662 |   27662 |  27662  ← Late Event 없는 윈도우
 2026-02-09 03:52:10 |  29074 |   29074 |  26743  ← naive 과소
 2026-02-09 03:52:20 |  32888 |   32888 |  40055  ← naive 과대
```

- **Flink = 정답**: Event Time + Watermark 덕분에 Late Event도 올바른 윈도우에 배치
- **Python ≠ 정답**: Processing Time 기준이므로 Late Event가 잘못된 윈도우에 배치

> **참고: 첫/마지막 윈도우 차이**
> - **첫 1~2개 윈도우**: Flink와 Consumer의 시작 시점 차이로 Flink 값이 더 작을 수 있음
> - **마지막 윈도우**: Flink의 Watermark(`event_time - 5초`) 때문에 아직 방출되지 않아 `0`으로 표시됨
> - 이것은 정상 동작입니다. Flink는 Late Event를 기다리기 위해 의도적으로 결과 방출을 지연합니다

### Flink가 해결한 것들

#### 1. Event Time 기반 처리
```sql
-- flink_job.py: Kafka Source DDL
CREATE TABLE payment_source (
    user_id STRING,
    amount INT,
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    ...
)
```
→ `event_time` 필드 기준으로 윈도우 분류 (도착 시간 무관)

#### 2. Watermark (Late Event 대응)
```sql
-- flink_job.py: TUMBLE Window 집계
INSERT INTO flink_results
SELECT window_start, window_end, user_id,
       CAST(SUM(amount) AS INT) AS total_amount,
       CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS updated_at
FROM TABLE(
    TUMBLE(TABLE payment_source, DESCRIPTOR(event_time), INTERVAL '10' SECOND)
)
GROUP BY window_start, window_end, user_id
```

**예시:**
```
윈도우: 00:00:00 ~ 00:00:10
Watermark 설정: event_time - 5초

이벤트 도착 순서:
  event_time=00:00:07 도착 → Watermark = 00:00:02 (윈도우 아직 열려있음)
  event_time=00:00:12 도착 → Watermark = 00:00:07 (윈도우 아직 열려있음)
  event_time=00:00:16 도착 → Watermark = 00:00:11 > 00:00:10 → 윈도우 닫힘!

이 시점에서 event_time 00:00:00~00:00:10 사이의 모든 이벤트가 집계되어 출력됨.
5초 늦게 도착한 이벤트도 Watermark 덕분에 올바른 윈도우에 포함됨.
```

#### 3. RocksDB + Checkpointing (상태 복구)
```python
# src/flink_job.py:37-58
env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(CHECKPOINT_INTERVAL_SECONDS * 1000)  # 10초마다 상태 스냅샷

conf = t_env.get_config().get_configuration()
conf.set_string("state.backend.type", "rocksdb")                    # 상태를 디스크(RocksDB)에 저장
conf.set_string("state.checkpoints.dir", CHECKPOINTS_DIR)           # 체크포인트 저장 경로
conf.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")     # 정확히 한 번 처리 보장
conf.set_string(
    "execution.checkpointing.externalized-checkpoint-retention",
    "RETAIN_ON_CANCELLATION",                                        # 잡 취소해도 체크포인트 보존
)
```

Python Consumer의 `window_sums = {}` (메모리)와 대조하면:
- **Python**: 프로세스 종료 → dict 소멸 → 집계 중인 데이터 유실
- **Flink**: 10초마다 RocksDB 상태를 체크포인트로 스냅샷 → 장애 시 마지막 체크포인트에서 복구

**테스트:**
```bash
# TaskManager 강제 종료
docker compose restart taskmanager

# 로그에서 체크포인트 복구 확인
docker compose logs taskmanager | grep "restore"
```

#### 4. UPSERT (결과 수정)
```sql
-- sql/init.sql: PK 설정으로 Upsert 지원
CREATE TABLE flink_results (
    window_start TIMESTAMP,
    window_end   TIMESTAMP,
    user_id      VARCHAR(50),
    total_amount INT,
    updated_at   TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (window_start, window_end, user_id)
);

-- flink_job.py: Sink DDL에서 PK 선언
CREATE TABLE flink_results (
    ...
    PRIMARY KEY (window_start, window_end, user_id) NOT ENFORCED
) WITH ('connector' = 'jdbc', ...)
```
→ Flink JDBC Connector가 PK 기반으로 자동 UPSERT 수행

### Web UI 탐색

Flink Web UI (http://localhost:8081)에서 확인할 것들:

1. **Running Jobs** 탭
   - Job Graph 시각화 (Source → Window → Sink)
   - Parallelism, Task 상태

2. **Task Metrics**
   - `numRecordsIn`: Kafka에서 읽은 레코드 수
   - `numRecordsOut`: PostgreSQL에 쓴 레코드 수
   - `currentWatermark`: 현재 Watermark 값

3. **Checkpoints** 탭
   - 체크포인트 성공/실패 이력
   - 상태 크기, 체크포인트 소요 시간

### Phase 4 정리
```bash
# 모든 서비스 중지
docker compose down
```

**배운 것**: Flink는 Event Time, Watermark, 상태 관리, UPSERT를 내장으로 제공하여, Late Event 환경에서도 정확한 실시간 집계를 보장한다.

---

## 핵심 개념 정리

### 1. Processing Time vs Event Time

| 구분 | Processing Time | Event Time |
|------|----------------|------------|
| **정의** | Consumer가 메시지를 받은 시간 | 이벤트가 실제 발생한 시간 |
| **Python 코드** | `datetime.now()` | `row["event_time"]` |
| **장점** | 구현 간단, 지연 없음 | 정확한 시간 기준 |
| **단점** | Late Event 시 부정확 | Watermark 설정 필요 |
| **사용 사례** | 모니터링, 단순 카운트 | 금융, 분석, SLA |

### 2. 상태 관리

| 구분 | Python dict | Flink RocksDB |
|------|------------|---------------|
| **저장 위치** | 메모리 (RAM) | 디스크 (persistent) |
| **프로세스 재시작** | 데이터 소실 | 체크포인트에서 복구 |
| **크기 제한** | RAM 크기 | 디스크 크기 (수백 GB 가능) |
| **성능** | 빠름 | 약간 느리지만 안정적 |

### 3. DB 쓰기 전략

| 구분 | INSERT | UPSERT |
|------|--------|--------|
| **동작** | 항상 새 행 추가 | 있으면 UPDATE, 없으면 INSERT |
| **중복** | 같은 윈도우 결과 중복 | Primary Key로 유일성 보장 |
| **Late Event 반영** | 불가능 (이미 INSERT됨) | 가능 (기존 행 UPDATE) |
| **SQL** | `INSERT INTO ...` | `INSERT ... ON CONFLICT DO UPDATE` |

### 4. Watermark 전략

```
Watermark = Event Time - Allowed Lateness

예: Watermark = event_time - 5초

┌─────────────────────────────────────────────────────┐
│           Window: 00:00:00 ~ 00:00:10               │
├─────────────────────────────────────────────────────┤
│                                                     │
│  ✅ event_time=00:00:03, 도착=00:00:08              │
│     → Watermark=00:00:03 → 윈도우에 포함           │
│                                                     │
│  ✅ event_time=00:00:09, 도착=00:00:15              │
│     → Watermark=00:00:10 → 윈도우 닫히기 직전 포함  │
│                                                     │
│  ❌ event_time=00:00:02, 도착=00:00:16              │
│     → Watermark=00:00:11 > 00:00:10 → 너무 늦음    │
│                                                     │
└─────────────────────────────────────────────────────┘

Watermark가 윈도우 끝(00:00:10)을 넘으면 윈도우 닫힘 → 결과 출력
```

**Watermark 설정 고려사항:**
- **짧게 (예: 1초)**: 결과 빨리 나오지만 Late Event 누락 위험
- **길게 (예: 1분)**: Late Event 대부분 포함하지만 결과 지연
- **실전**: 데이터 특성 분석 후 결정 (P95/P99 지연 시간 기준)

---

## 코드 읽기 가이드

### `src/producer.py` (Generator)
```python
# 핵심 구조 (src/producer.py)
# heapq(최소 힙)로 지연 전송을 관리한다
scheduled = []   # (전송_예정_시각, event) 튜플의 힙

while True:
    # 1. 예약 큐에서 전송 시각이 된 이벤트 발송
    while scheduled and scheduled[0][0] <= time.time():
        _, event = heapq.heappop(scheduled)
        send_event("[LATE]", event)

    # 2. 새 이벤트 생성
    event = {
        "user_id": f"U{random.randint(1, 5)}",
        "amount": random.randint(500, 5000),
        "event_time": datetime.now().isoformat(timespec="milliseconds"),
    }

    # 3. Chaos ON이면 20% 확률로 5초 지연 예약
    if CHAOS_ENABLED and random.random() < CHAOS_LATE_RATE:
        heapq.heappush(scheduled, (time.time() + CHAOS_DELAY_SECONDS, event))
    else:
        send_event("[NORMAL]", event)

    time.sleep(EVENT_INTERVAL_SECONDS)  # 1초 대기
```

**주요 포인트:**
- `event_time`은 생성 시점의 타임스탬프 → Late Event여도 event_time은 과거 시점
- Chaos 모드: 20%의 이벤트를 heapq에 담아 5초 후 발송
- heapq는 `(send_time, event)` 튜플을 시간 기준으로 자동 정렬하는 최소 힙

### `src/naive_consumer.py` (Python Consumer)
```python
# 핵심 구조 (src/naive_consumer.py)
WINDOW_SECONDS = 10
window_sums = {}   # {user_id: total_amount} - 메모리에만 존재!

current_window_start = floor_window_start(datetime.now())

while True:
    now = datetime.now()

    # Processing Time 기준으로 10초마다 윈도우 닫기
    while now >= current_window_start + timedelta(seconds=WINDOW_SECONDS):
        flush_window(conn, current_window_start, window_sums, RESULT_TABLE)
        window_sums = {}
        current_window_start += timedelta(seconds=WINDOW_SECONDS)

    msg = consumer.poll(1.0)
    event = json.loads(msg.value())

    # 의도된 결함: event["event_time"]을 무시하고 Processing Time 윈도우에 누적
    window_sums[event["user_id"]] = window_sums.get(event["user_id"], 0) + event["amount"]
```

**문제점:**
- `datetime.now()` 기준으로 윈도우를 결정 → 늦게 도착한 이벤트는 잘못된 윈도우에 배치
- `window_sums`가 Python dict(메모리) → 프로세스 종료 시 유실
- 종료 시 flush하지 않음 → 마지막 윈도우 데이터 손실

### `src/flink_job.py` (PyFlink Job)
```python
# 핵심 구조 (src/flink_job.py) - Table SQL API 방식

# 1. Kafka Source (Event Time + Watermark 선언)
t_env.execute_sql("""
    CREATE TABLE payment_source (
        user_id STRING,
        amount INT,
        event_time TIMESTAMP(3),
        WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'payment-log',
        'format' = 'json',
        'json.timestamp-format.standard' = 'ISO-8601',
        ...
    )
""")

# 2. JDBC Sink (PK 기반 Upsert)
t_env.execute_sql("""
    CREATE TABLE flink_results (
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        user_id STRING,
        total_amount INT,
        updated_at TIMESTAMP(3),
        PRIMARY KEY (window_start, window_end, user_id) NOT ENFORCED
    ) WITH ('connector' = 'jdbc', 'url' = '...', ...)
""")

# 3. Event Time 기반 TUMBLE Window 집계 → Sink로 INSERT
t_env.execute_sql("""
    INSERT INTO flink_results
    SELECT window_start, window_end, user_id,
           CAST(SUM(amount) AS INT),
           CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3))
    FROM TABLE(
        TUMBLE(TABLE payment_source, DESCRIPTOR(event_time), INTERVAL '10' SECOND)
    )
    GROUP BY window_start, window_end, user_id
""").wait()
```

**주요 포인트:**
- `WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND`: 5초까지 Late Event 허용
- `TUMBLE(..., DESCRIPTOR(event_time), ...)`: Event Time 기준 10초 윈도우
- `PRIMARY KEY ... NOT ENFORCED`: JDBC Sink가 자동으로 UPSERT 수행
- `json.timestamp-format.standard = ISO-8601`: Producer의 ISO 형식 타임스탬프 파싱

### `sql/init.sql` (DB 스키마)
```sql
-- baseline_results / naive_results: PK 없음 (INSERT만)
CREATE TABLE baseline_results (
    window_start TIMESTAMP,
    user_id      VARCHAR(50),
    total_amount INT,
    updated_at   TIMESTAMP DEFAULT NOW()
);

-- flink_results: PK 있음 (UPSERT 지원)
CREATE TABLE flink_results (
    window_start TIMESTAMP,
    window_end   TIMESTAMP,
    user_id      VARCHAR(50),
    total_amount INT,
    updated_at   TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (window_start, window_end, user_id)
);
```

---

## 트러블슈팅

### 문제 1: Kafka가 시작되지 않음
```bash
# 증상
docker compose logs kafka
# "Connection to node -1 could not be established"

# 해결
docker compose down -v  # 볼륨 삭제
docker compose up -d kafka
# 30초 대기 후 재시도
```

### 문제 2: PostgreSQL 연결 실패
```bash
# 증상
FATAL: password authentication failed for user "postgres"

# 해결
docker compose exec postgres psql -U postgres
# 프롬프트 나오는지 확인

# 비밀번호 확인
docker compose exec postgres env | grep POSTGRES_PASSWORD
```

### 문제 3: Flink Job 제출 실패
```bash
# 증상
The program finished with the following exception:
java.lang.ClassNotFoundException: org.apache.flink.connector.jdbc...

# 원인: Connector JAR가 없음

# 해결
docker compose down
docker compose build --no-cache jobmanager taskmanager
docker compose up -d jobmanager taskmanager
```

### 문제 4: Python Consumer가 데이터를 받지 못함
```bash
# 디버깅
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic payment-log \
  --from-beginning

# 메시지가 보이면 Kafka는 정상
# Python Consumer 로그 확인
docker compose logs python-consumer | grep WARN
```

### 문제 5: Flink Web UI 접속 안 됨
```bash
# Windows에서 WSL2 포트 포워딩 확인
netsh interface portproxy show all

# 없으면 수동 포워딩
netsh interface portproxy add v4tov4 \
  listenport=8081 listenaddress=0.0.0.0 \
  connectport=8081 connectaddress=172.x.x.x  # WSL2 IP
```

### 문제 6: Late Event가 너무 많이 발생
```bash
# CHAOS_LATE_RATE 조정 (기본 0.2 = 20%)
CHAOS_ENABLED=true CHAOS_LATE_RATE=0.1 \
  docker compose up -d generator

# CHAOS_DELAY_SECONDS 조정 (기본 5초)
CHAOS_ENABLED=true CHAOS_DELAY_SECONDS=3 \
  docker compose up -d generator
```

### 문제 7: 디스크 공간 부족
```bash
# Docker 리소스 정리
docker system prune -a --volumes

# 이미지 재다운로드
docker compose pull
```

---

## 실습 체크리스트

### Phase 1
- [ ] Kafka, PostgreSQL 시작 확인
- [ ] Generator 로그에서 이벤트 전송 확인
- [ ] Python Consumer 로그에서 윈도우 닫힘 확인
- [ ] baseline_results 테이블에 데이터 INSERT 확인
- [ ] 윈도우별 집계 결과가 정확한지 확인

### Phase 2
- [ ] Chaos 모드 활성화 확인 (로그에서 "Late event scheduled")
- [ ] naive_results와 baseline_results 총합 비교
- [ ] 특정 윈도우에서 차이 발생 확인
- [ ] Late Event가 잘못된 윈도우에 집계된 것을 확인

### Phase 3
- [ ] Event Time vs Processing Time 차이 이해
- [ ] Watermark 개념 이해
- [ ] 상태 관리 필요성 이해
- [ ] UPSERT 필요성 이해

### Phase 4
- [ ] Flink 클러스터 시작 확인 (Web UI 접속)
- [ ] Flink Job 제출 성공 확인
- [ ] flink_results 테이블에 데이터 UPSERT 확인
- [ ] Chaos 모드에서도 flink_results가 정확한 것 확인
- [ ] baseline_results (Chaos OFF)와 flink_results (Chaos ON) 결과 일치 확인

---

## 추가 학습 자료

### Flink 공식 문서
- [Event Time and Watermarks](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/concepts/time/)
- [State Backends](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/state/state_backends/)
- [Checkpointing](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/fault-tolerance/checkpointing/)

### 실전 활용
1. **Windowing 변형**
   - Tumbling (고정 윈도우): 00:00-00:10, 00:10-00:20
   - Sliding (슬라이딩 윈도우): 00:00-00:10, 00:05-00:15 (중복)
   - Session (세션 윈도우): 이벤트 간격이 5초 이상이면 윈도우 분리

2. **Watermark 전략**
   - Periodic: 주기적으로 생성 (대부분 사용)
   - Punctuated: 특정 이벤트 기준 생성

3. **Exactly-Once 보장**
   - Flink Checkpoint + Kafka Transaction
   - Two-Phase Commit (2PC)

### 실습 확장 아이디어
1. **사용자 행동 분석**: 세션 윈도우로 구매 패턴 분석
2. **이상 탐지**: 윈도우 내 금액 합계가 임계값 초과 시 알림
3. **Late Event 모니터링**: Watermark 초과 이벤트 수 집계
4. **다중 Sink**: PostgreSQL + Kafka (재가공용) 동시 출력

---

## Q&A

**Q: Watermark를 너무 길게 설정하면 안 되나요?**
A: 결과가 지연됩니다. 예를 들어 Watermark를 1분으로 설정하면, 윈도우가 닫히려면 event_time이 윈도우 끝 + 1분이 되어야 합니다. 실시간 대시보드에는 부적합합니다.

**Q: Late Event를 완전히 없앨 수는 없나요?**
A: 불가능합니다. 네트워크 지연, GC pause, 시계 불일치 등은 필연적입니다. 대신 Watermark로 "얼마나 기다릴지" 조율합니다.

**Q: Python Consumer를 Flink처럼 구현할 수는 없나요?**
A: 가능하지만, Event Time 처리, Watermark, 상태 관리, Checkpointing, Exactly-Once를 모두 직접 구현해야 합니다. Flink는 이를 프레임워크로 제공합니다.

**Q: Flink 대신 Spark Streaming은 안 되나요?**
A: Spark Streaming도 가능하지만, Micro-batch 방식이라 latency가 더 높습니다. Flink는 True Streaming (event-by-event)으로 더 낮은 latency를 제공합니다.

**Q: 실제 운영 환경에서는 어떻게 배포하나요?**
A: Kubernetes 위에 Flink Operator를 사용하거나, AWS Kinesis Data Analytics, Azure Stream Analytics 같은 Managed Service를 사용합니다.

---

## 실습 후 정리

이 실습을 통해 배운 것:

1. **Python만으로도 실시간 처리가 가능하다** (Phase 1)
2. **그러나 운영 환경(Late Event)에서는 한계가 있다** (Phase 2)
3. **정확한 처리를 위해서는 Event Time, Watermark, 상태 관리, UPSERT가 필요하다** (Phase 3)
4. **Flink는 이런 기능들을 프레임워크로 제공한다** (Phase 4)

실시간 데이터 처리는 단순히 "빠르게 처리하는 것"이 아니라, **정확성(Correctness)**과 **복구성(Fault Tolerance)**을 보장하는 것입니다. 이것이 Flink 같은 전문 프레임워크가 필요한 이유입니다.

---

## 전체 정리
```bash
# 모든 컨테이너 중지 및 삭제
docker compose down

# 볼륨까지 삭제 (DB 데이터 초기화)
docker compose down -v

# 이미지까지 삭제 (재빌드 필요 시)
docker compose down --rmi all -v
```
