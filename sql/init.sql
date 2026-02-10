-- ================================================
-- stream-lab: 기본 처리 vs 한계 노출 vs Flink 비교용 테이블
-- ================================================

-- 1. Baseline 결과
-- "기본적으로 실시간 처리는 가능하다"를 보여주는 단계의 결과 저장용
CREATE TABLE baseline_results (
    window_start TIMESTAMP,
    user_id      VARCHAR(50),
    total_amount INT,
    updated_at   TIMESTAMP DEFAULT NOW()
);

-- 2. Python Processing-Time 방식의 한계 노출 결과
-- Event Time이 늦게 도착하면 window_start(처리 시점) 기준으로 잘못 묶일 수 있다.
CREATE TABLE naive_results (
    window_start TIMESTAMP,
    user_id      VARCHAR(50),
    total_amount INT,
    updated_at   TIMESTAMP DEFAULT NOW()
);

-- 3. 개별 이벤트 로그 (Processing Time vs Event Time 비교용)
-- Phase 2에서 각 이벤트가 어느 윈도우에 배치되었는지(assigned_window)와
-- 실제로 어느 윈도우에 속해야 하는지(correct_window)를 함께 기록합니다.
CREATE TABLE IF NOT EXISTS event_log (
    event_time      TIMESTAMP NOT NULL,
    proc_time       TIMESTAMP NOT NULL,
    assigned_window TIMESTAMP NOT NULL,
    correct_window  TIMESTAMP NOT NULL,
    user_id         TEXT NOT NULL,
    amount          BIGINT NOT NULL
);

-- 4. Flink 방식의 결과
-- Event Time 기반이라 window_start/end가 "데이터 발생 시간"을 의미
-- PK로 Upsert 지원 (같은 윈도우+유저 조합은 덮어쓰기)
CREATE TABLE flink_results (
    window_start TIMESTAMP,
    window_end   TIMESTAMP,
    user_id      VARCHAR(50),
    total_amount INT,
    updated_at   TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (window_start, window_end, user_id)
);

-- 5. Spark Structured Streaming 결과 (Bonus: Flink vs Spark 비교용)
-- Micro-batch 모드로 동일한 윈도우 집계를 수행한 결과
-- Flink 결과와 레이턴시/정확성을 비교할 수 있다
CREATE TABLE spark_results (
    window_start TIMESTAMP,
    window_end   TIMESTAMP,
    user_id      VARCHAR(50),
    total_amount INT,
    updated_at   TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (window_start, window_end, user_id)
);

-- 6. Fraud Detection 결과
-- DataStream API 기반 사기 탐지 알림 저장
-- "소액 결제 후 1분 이내 고액 결제" 패턴이 감지되면 기록됨
CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id     SERIAL,
    user_id      VARCHAR(50) NOT NULL,
    small_amount INT NOT NULL,
    large_amount INT NOT NULL,
    alert_time   TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (alert_id)
);
