"""
Fraud Detection with PyFlink DataStream API
============================================

# ===== Table API vs DataStream API 비교 =====
#
# flink_job.py (Table API / SQL):
#   - SQL 기반 선언적 처리: CREATE TABLE → INSERT INTO SELECT
#   - 윈도우 집계(TUMBLE), 워터마크 등 "무엇을"만 기술
#   - Flink가 실행 계획을 자동 최적화
#   - 적합한 경우: 윈도우 집계, JOIN, GROUP BY 같은 관계형 연산
#
# flink_fraud.py (DataStream API):
#   - 명령형 처리: KeyedProcessFunction으로 이벤트를 하나씩 직접 처리
#   - "어떻게" 처리할지 개발자가 상태(State)와 타이머(Timer)를 직접 관리
#   - 커스텀 비즈니스 로직 구현에 유연
#   - 적합한 경우: 복잡한 이벤트 패턴 탐지, CEP(Complex Event Processing)
#
# 왜 이 파일이 필요한가:
#   Table API만으로는 "소액 결제 후 1분 이내 고액 결제" 같은
#   상태 기반(Stateful) 패턴 매칭을 표현하기 어렵다.
#   DataStream API의 KeyedProcessFunction을 쓰면:
#     1. 사용자별 상태(ValueState)에 직전 소액 결제를 기억하고
#     2. 타이머(Timer)로 1분 만료를 관리하며
#     3. 고액 결제가 들어오면 상태를 확인해 사기 여부를 판단
#   이런 "이벤트 간 관계 추론"이 가능하다.
# ================================================================

"""

import os
import json

from pyflink.common import Types, WatermarkStrategy, Row
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.connectors.jdbc import (
    JdbcSink,
    JdbcConnectionOptions,
    JdbcExecutionOptions,
)
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext, MapFunction
from pyflink.datastream.state import ValueStateDescriptor

# ===== 설정 =====
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "fraud-payments")

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "streamdb")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# 왜: 이 임계값들이 사기 패턴의 정의다.
# fraud_generator.py에서 소액=100~900, 고액=15000~50000으로 생성하므로
# 이 임계값으로 정확히 탐지할 수 있다.
SMALL_AMOUNT_THRESHOLD = 1000   # 이 미만이면 "소액 결제"
LARGE_AMOUNT_THRESHOLD = 10000  # 이 이상이면 "고액 결제"
TIMER_DURATION_MS = 60 * 1000   # 1분 (소액→고액 패턴의 시간 윈도우)


# ===== JSON 파싱 함수 =====
# 왜: Kafka에서 읽은 원시 문자열을 구조화된 Row로 변환해야
# KeyedProcessFunction에서 user_id, amount 등의 필드에 접근할 수 있다.
class ParseEventFunction(MapFunction):
    def map(self, value):
        data = json.loads(value)
        return Row(
            user_id=str(data.get("user_id", "")),
            amount=int(data.get("amount", 0)),
            event_time=str(data.get("event_time", "")),
        )


# ===== 핵심: 사기 탐지 KeyedProcessFunction =====
#
# KeyedProcessFunction이란?
#   - key_by()로 분류된 각 키(여기서는 user_id)마다 독립적으로 동작
#   - 키별 상태(ValueState)를 유지 → U1의 상태와 U2의 상태는 완전히 분리
#   - Processing Time Timer를 등록해 미래 시점에 콜백을 받을 수 있음
#
# 탐지 로직:
#   1. 소액 결제(<1000) 도착 → 상태에 금액 저장 + 1분 타이머 등록
#   2. 고액 결제(>10000) 도착 + 상태에 소액 기록 있음 → FRAUD ALERT!
#   3. 타이머 만료(1분 경과) → 소액 기록 삭제 (패턴 실패, 정상으로 간주)
#
# 왜 이 방식이 효과적인가:
#   - Flink가 상태를 Checkpoint로 관리하므로 장애 시에도 상태가 유실되지 않음
#   - key_by(user_id) 덕분에 수백만 사용자를 병렬 처리 가능
#   - 타이머가 자동으로 패턴 만료를 관리 → 메모리 누수 방지
# ================================================================
class FraudDetector(KeyedProcessFunction):

    def __init__(self):
        # 왜: __init__에서는 선언만 한다.
        # 실제 State 초기화는 open()에서 해야 Flink 런타임이 관리하는 State를 받을 수 있다.
        self._small_amount_state = None  # 소액 결제 금액 기억용
        self._timer_state = None         # 등록한 타이머 시각 기억용

    def open(self, runtime_context: RuntimeContext):
        # 왜: ValueStateDescriptor로 Flink 관리 상태를 생성한다.
        # 이 상태는 Checkpoint에 포함되어 장애 복구 시에도 유지된다.
        #
        # Table API(flink_job.py)에서는 이런 상태 관리가 SQL 뒤에 숨겨져 있지만,
        # DataStream API에서는 개발자가 직접 선언하고 관리한다.
        self._small_amount_state = runtime_context.get_state(
            ValueStateDescriptor("small_amount", Types.INT())
        )
        self._timer_state = runtime_context.get_state(
            ValueStateDescriptor("timer_ts", Types.LONG())
        )

    def process_element(self, value, ctx: KeyedProcessFunction.Context):
        """
        이벤트가 도착할 때마다 호출된다.
        value: Row(user_id, amount, event_time)
        ctx: 타이머 서비스, 현재 키 등에 접근할 수 있는 컨텍스트
        """
        amount = value.amount
        user_id = value.user_id

        # ── 케이스 1: 고액 결제 도착 ──
        if amount >= LARGE_AMOUNT_THRESHOLD:
            small_amount = self._small_amount_state.value()
            if small_amount is not None:
                # ===== FRAUD DETECTED! =====
                # 왜: 상태에 소액 기록이 있다 = 이전에 소액 결제가 있었고, 아직 1분이 안 지남
                # → "소액 후 고액" 패턴 성립 → 사기 알림 발생
                print(
                    f"[ALERT] 사기 탐지! user={user_id} "
                    f"소액={small_amount}원 → 고액={amount}원"
                )
                yield Row(user_id=user_id, small_amount=small_amount, large_amount=amount)

            # 왜: 사기든 아니든, 고액 결제 후에는 상태를 정리한다.
            # 같은 사용자의 다음 패턴을 새로 추적하기 위함.
            self._clean_up(ctx.timer_service())

        # ── 케이스 2: 소액 결제 도착 ──
        if amount < SMALL_AMOUNT_THRESHOLD:
            # 왜: 소액 결제를 상태에 기록하고, 1분 타이머를 등록한다.
            # 이 타이머가 만료되기 전에 고액 결제가 오면 → 사기
            # 타이머가 먼저 만료되면 → 정상 (on_timer에서 상태 정리)
            self._small_amount_state.update(amount)
            timer_ts = ctx.timer_service().current_processing_time() + TIMER_DURATION_MS
            ctx.timer_service().register_processing_time_timer(timer_ts)
            self._timer_state.update(timer_ts)

    def on_timer(self, timestamp: int, ctx: KeyedProcessFunction.OnTimerContext):
        """
        등록한 타이머가 만료되면 호출된다.
        왜: 소액 결제 후 1분이 지나도 고액 결제가 없으면 정상 거래로 간주.
        상태를 정리하여 메모리를 확보한다.
        """
        self._small_amount_state.clear()
        self._timer_state.clear()

    def _clean_up(self, timer_service):
        """상태와 타이머를 모두 정리하는 헬퍼 메서드"""
        timer_ts = self._timer_state.value()
        if timer_ts is not None:
            timer_service.delete_processing_time_timer(timer_ts)
        self._small_amount_state.clear()
        self._timer_state.clear()


def main():
    print("[INFO] Fraud Detection Job 시작 (DataStream API)")
    print(f"[INFO] Kafka: servers={KAFKA_BOOTSTRAP_SERVERS} topic={KAFKA_TOPIC}")
    print(f"[INFO] Postgres JDBC URL: {JDBC_URL}")
    print(
        f"[INFO] 탐지 규칙: 소액(<{SMALL_AMOUNT_THRESHOLD}) 후 "
        f"{TIMER_DURATION_MS // 1000}초 이내 고액(>={LARGE_AMOUNT_THRESHOLD}) → ALERT"
    )

    # ===== 1. 실행 환경 설정 =====
    env = StreamExecutionEnvironment.get_execution_environment()
    # 왜: STREAMING 모드를 명시적으로 설정한다.
    # DataStream API의 기본 모드이지만, 교육 목적으로 명시.
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    # 왜: 장애 복구용 체크포인트. flink_job.py(Table API)와 동일한 설정.
    env.enable_checkpointing(10 * 1000)

    # ===== 2. Kafka Source =====
    # 왜: DataStream API에서는 KafkaSource 빌더로 소스를 직접 구성한다.
    # Table API(flink_job.py)에서는 CREATE TABLE DDL로 선언했던 것과 대비된다.
    #
    # Table API:  CREATE TABLE payment_source (...) WITH ('connector' = 'kafka', ...)
    # DataStream: KafkaSource.builder().set_topics(...).build()
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(KAFKA_TOPIC)
        .set_group_id("flink-fraud-detector")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # 왜: WatermarkStrategy.no_watermarks()를 사용한다.
    # 이 잡은 Processing Time 기반 타이머를 사용하므로 Event Time Watermark가 필요 없다.
    # flink_job.py(Table API)에서는 WATERMARK FOR event_time으로 Event Time을 쓰지만,
    # 여기서는 "소액 후 1분 이내 고액"이라는 Processing Time 기반 패턴을 탐지한다.
    ds = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Fraud Payments Source",
    )

    # ===== 3. JSON 파싱 =====
    # 왜: Kafka에서 읽은 raw JSON 문자열을 Row(user_id, amount, event_time)으로 변환.
    # Table API에서는 'format' = 'json'으로 자동 파싱되지만,
    # DataStream API에서는 MapFunction으로 직접 파싱해야 한다.
    parsed = ds.map(
        ParseEventFunction(),
        output_type=Types.ROW_NAMED(
            ["user_id", "amount", "event_time"],
            [Types.STRING(), Types.INT(), Types.STRING()],
        ),
    )

    # ===== 4. KeyBy + FraudDetector =====
    # 왜: key_by(user_id)로 같은 사용자의 이벤트를 같은 파티션으로 모은다.
    # 이렇게 해야 FraudDetector가 사용자별 독립 상태를 유지할 수 있다.
    #
    # Table API에서 GROUP BY user_id와 비슷하지만,
    # 여기서는 집계가 아니라 "이벤트 간 패턴 매칭"을 수행한다.
    alerts = parsed.key_by(lambda row: row.user_id).process(
        FraudDetector(),
        output_type=Types.ROW_NAMED(
            ["user_id", "small_amount", "large_amount"],
            [Types.STRING(), Types.INT(), Types.INT()],
        ),
    )

    # ===== 5. Print Sink (콘솔 출력) =====
    # 왜: 수업 시연 시 docker compose logs로 실시간 알림을 확인할 수 있다.
    alerts.print()

    # ===== 6. JDBC Sink (PostgreSQL 저장) =====
    # 왜: 탐지된 사기 알림을 fraud_alerts 테이블에 저장한다.
    # Table API에서는 CREATE TABLE sink_ddl WITH ('connector'='jdbc')로 선언했지만,
    # DataStream API에서는 JdbcSink.sink()로 직접 구성한다.
    #
    # Table API:  INSERT INTO flink_results SELECT ...
    # DataStream: alerts.add_sink(JdbcSink.sink("INSERT INTO ...", ...))
    jdbc_sink = JdbcSink.sink(
        "INSERT INTO fraud_alerts (user_id, small_amount, large_amount) VALUES (?, ?, ?)",
        type_info=Types.ROW_NAMED(
            ["user_id", "small_amount", "large_amount"],
            [Types.STRING(), Types.INT(), Types.INT()],
        ),
        jdbc_connection_options=(
            JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .with_url(JDBC_URL)
            .with_driver_name("org.postgresql.Driver")
            .with_user_name(POSTGRES_USER)
            .with_password(POSTGRES_PASSWORD)
            .build()
        ),
        jdbc_execution_options=(
            JdbcExecutionOptions.builder()
            .with_batch_interval_ms(1000)
            .with_batch_size(10)
            .with_max_retries(3)
            .build()
        ),
    )
    alerts.add_sink(jdbc_sink)

    # ===== 7. 실행 =====
    # 왜: DataStream API에서는 env.execute()로 잡을 제출한다.
    # Table API에서는 t_env.execute_sql(insert_sql).wait()로 실행했던 것과 대비.
    print("[INFO] Fraud Detection Job 제출 중...")
    env.execute("Fraud Detection Job")


if __name__ == "__main__":
    main()
