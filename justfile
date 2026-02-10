# ============================================================
# stream-lab Justfile
# Phase별 실습을 깔끔하게 실행/정리하기 위한 커맨드 모음
#
# 사용법:  just <recipe>
# 목록:    just --list
# ============================================================

# 기본값: just만 치면 레시피 목록 출력
default:
    @just --list

# ─────────────────────────────────────────────
# 공통 유틸
# ─────────────────────────────────────────────

# 모든 컨테이너 + 볼륨 + 네트워크를 완전히 삭제 (초기 상태로)
clean:
    docker compose down -v --remove-orphans

# Kafka, PostgreSQL 시작 및 healthcheck 대기
infra:
    #!/usr/bin/env bash
    set -euo pipefail
    docker compose up -d kafka postgres
    echo "⏳ Kafka·Postgres healthcheck 대기 중..."
    for i in $(seq 1 20); do
        kafka_ok=false
        postgres_ok=false
        docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server localhost:9092 --list > /dev/null 2>&1 && kafka_ok=true
        docker compose exec -T postgres pg_isready -U postgres > /dev/null 2>&1 && postgres_ok=true
        if $kafka_ok && $postgres_ok; then
            echo "✅ Kafka·Postgres 준비 완료"
            exit 0
        fi
        echo "  대기 중... (${i}/20)"
        sleep 3
    done
    echo "❌ Healthcheck 타임아웃 (60초)"
    exit 1

# DB 테이블 전체 초기화
reset-db:
    docker compose exec postgres psql -U postgres -d streamdb \
        -c "TRUNCATE baseline_results, naive_results, event_log, flink_results, spark_results, fraud_alerts;"

# 실행 중인 모든 Flink Job 취소
cancel-flink-jobs:
    #!/usr/bin/env bash
    set -euo pipefail
    jobs=$(docker compose exec -T jobmanager flink list 2>/dev/null \
        | grep -oE '[a-f0-9]{32}' || true)
    if [ -z "$jobs" ]; then
        echo "ℹ️  실행 중인 Flink Job이 없습니다."
    else
        for jid in $jobs; do
            echo "🛑 Job $jid 취소 중..."
            docker compose exec -T jobmanager flink cancel "$jid" 2>/dev/null || true
        done
        echo "✅ 모든 Flink Job이 취소되었습니다."
    fi

# Generator, Python Consumer 중지
stop-apps:
    docker compose stop generator python-consumer fraud-generator 2>/dev/null || true

# ─────────────────────────────────────────────
# Phase 1: 기본 처리 가능성 확인 (Chaos OFF)
# ─────────────────────────────────────────────

# Phase 1 실행: 인프라 시작 → Generator + Consumer (Chaos OFF, baseline_results)
phase1: clean infra
    @echo ""
    @echo "🚀 Phase 1: Chaos OFF / baseline_results"
    @echo "   1분 후 just query-phase1 로 결과를 확인하세요."
    @echo ""
    docker compose up -d generator python-consumer

# Phase 1 결과 조회
query-phase1:
    @echo "── baseline_results (상위 20행) ──"
    @docker compose exec postgres psql -U postgres -d streamdb \
        -c "SELECT window_start, user_id, total_amount, updated_at FROM baseline_results ORDER BY window_start, user_id LIMIT 20;"
    @echo ""
    @echo "── 윈도우별 요약 ──"
    @docker compose exec postgres psql -U postgres -d streamdb \
        -c "SELECT window_start, COUNT(*) as user_count, SUM(total_amount) as total FROM baseline_results GROUP BY window_start ORDER BY window_start;"

# Phase 1 정리 후 Phase 2 준비
phase1-done:
    @echo "🧹 Phase 1 정리 중..."
    docker compose stop generator python-consumer
    @echo "✅ Phase 1 완료. just phase2 로 다음 단계를 시작하세요."

# ─────────────────────────────────────────────
# Phase 2: 운영 한계 노출 (Chaos ON)
# ─────────────────────────────────────────────

# Phase 2 실행: Chaos ON → Late Event 발생 → naive_results 저장
phase2: stop-apps
    @echo ""
    @echo "🌪️ Phase 2: Chaos ON / naive_results"
    @echo "   1분 후 just query-phase2 로 결과를 확인하세요."
    @echo ""
    CHAOS_ENABLED=true RESULT_TABLE=naive_results KAFKA_GROUP_ID=python-consumer-limit \
        docker compose up -d generator python-consumer

# Phase 2 결과 조회: Processing Time vs Event Time 비교
query-phase2:
    @echo "── 전체 합계 비교 (동일해야 함) ──"
    @docker compose exec postgres psql -U postgres -d streamdb \
        -c "SELECT 'Processing Time 기준' AS method, SUM(amount) AS grand_total FROM event_log UNION ALL SELECT 'Event Time 기준', SUM(amount) FROM event_log;"
    @echo ""
    @echo "── 윈도우별 비교 (diff ≠ 0 = Late Event 영향) ──"
    @docker compose exec postgres psql -U postgres -d streamdb \
        -c "SELECT COALESCE(p.window_start, e.window_start) AS window_start, COALESCE(p.proc_total, 0) AS proc_total, COALESCE(e.event_total, 0) AS event_total, COALESCE(p.proc_total, 0) - COALESCE(e.event_total, 0) AS diff FROM (SELECT assigned_window AS window_start, SUM(amount) AS proc_total FROM event_log GROUP BY assigned_window) p FULL OUTER JOIN (SELECT correct_window AS window_start, SUM(amount) AS event_total FROM event_log GROUP BY correct_window) e ON p.window_start = e.window_start ORDER BY window_start;"

# Phase 2 Late Event 로그 확인
late-events:
    @echo "── Generator의 Late Event 예약 ──"
    @docker compose logs generator 2>&1 | grep "LATE" | tail -10
    @echo ""
    @echo "── Consumer의 Late Event 오배치 감지 ──"
    @docker compose logs python-consumer 2>&1 | grep "LATE-DETECT" | tail -10

# Phase 2 정리 후 Phase 4 준비
phase2-done:
    @echo "🧹 Phase 2 정리 중..."
    docker compose stop generator python-consumer
    @echo "✅ Phase 2 완료. just phase4 로 다음 단계를 시작하세요."

# ─────────────────────────────────────────────
# Phase 4: Flink 비교 실행
# ─────────────────────────────────────────────

# Phase 4 실행: 전체 초기화 → 인프라 + Flink + Generator + Consumer 한 번에 시작
phase4: clean infra _phase4-start

# Phase 4 내부: 토픽 생성 → Flink Job 제출 → Generator + Consumer 동시 시작
_phase4-start:
    #!/usr/bin/env bash
    set -euo pipefail

    echo ""
    echo "🔧 Phase 4: Flink + Chaos ON 비교"
    echo ""

    # 1. Flink 클러스터 시작
    echo "⚙️  Flink 클러스터 시작..."
    docker compose up -d jobmanager taskmanager

    # 2. Kafka 토픽 미리 생성 (Flink Job이 토픽 없음 에러를 내지 않도록)
    echo "📡 Kafka 토픽(payment-log) 생성..."
    docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --create --topic payment-log \
        --partitions 1 --replication-factor 1 \
        --if-not-exists

    # 3. Flink 클러스터 준비 대기
    echo "⏳ Flink 클러스터 준비 대기 (40초)..."
    sleep 40

    # 4. 기존 Flink Job이 있으면 모두 취소
    jobs=$(docker compose exec -T jobmanager flink list 2>/dev/null \
        | grep -oE '[a-f0-9]{32}' || true)
    for jid in $jobs; do
        echo "🛑 기존 Job $jid 취소..."
        docker compose exec -T jobmanager flink cancel "$jid" 2>/dev/null || true
    done

    # 5. Flink Job 제출 (Generator보다 먼저 → 첫 이벤트부터 수신)
    echo "🚀 Flink Job 제출..."
    docker compose exec -T jobmanager flink run -d -py /opt/flink/src/flink_job.py
    sleep 5  # Job이 RUNNING 상태로 전환될 때까지 잠시 대기

    # 6. Generator + Python Consumer 동시 시작 (같은 시점부터 이벤트 생산/소비)
    echo ""
    echo "📡 Generator + Python Consumer 동시 시작..."
    CHAOS_ENABLED=true RESULT_TABLE=naive_results KAFKA_GROUP_ID=python-consumer-flink \
        docker compose up -d generator python-consumer

    echo ""
    echo "✅ Phase 4 실행 중!"
    echo "   1~2분 후 just query-phase4 로 결과를 비교하세요."
    echo "   Flink Web UI: http://localhost:8081"

# Phase 4 결과 조회: Flink vs 정답 vs Python 비교
query-phase4:
    @echo "── 전체 합계: Flink vs 정답 ──"
    @docker compose exec postgres psql -U postgres -d streamdb \
        -c "SELECT 'Flink (Event Time)' AS method, SUM(total_amount) AS grand_total FROM flink_results UNION ALL SELECT '정답 (Event Time)', SUM(amount) FROM event_log;"
    @echo ""
    @echo "── 윈도우별 비교: Flink = 정답, Python ≠ 정답 ──"
    @docker compose exec postgres psql -U postgres -d streamdb \
        -c "SELECT COALESCE(f.window_start, e.window_start) AS window_start, COALESCE(f.flink_total, 0) AS \"Flink\", COALESCE(e.event_total, 0) AS \"정답(Event Time)\", COALESCE(p.proc_total, 0) AS \"Python(Proc Time)\" FROM (SELECT window_start, SUM(total_amount) AS flink_total FROM flink_results GROUP BY window_start) f FULL OUTER JOIN (SELECT correct_window AS window_start, SUM(amount) AS event_total FROM event_log GROUP BY correct_window) e ON f.window_start = e.window_start FULL OUTER JOIN (SELECT assigned_window AS window_start, SUM(amount) AS proc_total FROM event_log GROUP BY assigned_window) p ON COALESCE(f.window_start, e.window_start) = p.window_start ORDER BY window_start;"

# Flink 결과만 조회
query-flink:
    @docker compose exec postgres psql -U postgres -d streamdb \
        -c "SELECT window_start, window_end, user_id, total_amount, updated_at FROM flink_results ORDER BY window_start, user_id LIMIT 20;"

# ─────────────────────────────────────────────
# Bonus: Spark vs Flink 비교
# ─────────────────────────────────────────────

# Bonus 실행: Phase 4 환경에 Spark를 추가로 시작
bonus-spark:
    @echo ""
    @echo "⚡ Bonus: Spark Structured Streaming 시작"
    @echo "   Phase 4가 실행 중인 상태에서 사용하세요."
    @echo ""
    docker compose up -d spark-submit
    @echo ""
    @echo "✅ Spark 시작됨. 2분 후 just query-bonus 로 비교하세요."

# Bonus 결과 조회: Flink vs Spark vs 정답 vs Python 4자 비교
query-bonus:
    @echo "── 레이턴시 비교: Flink vs Spark ──"
    @docker compose exec postgres psql -U postgres -d streamdb \
        -c "SELECT 'Flink' AS engine, ROUND(AVG(EXTRACT(EPOCH FROM (updated_at - window_end)))::numeric, 3) AS avg_latency_sec, ROUND(MIN(EXTRACT(EPOCH FROM (updated_at - window_end)))::numeric, 3) AS min_latency_sec, ROUND(MAX(EXTRACT(EPOCH FROM (updated_at - window_end)))::numeric, 3) AS max_latency_sec, COUNT(*) AS window_count FROM flink_results WHERE updated_at IS NOT NULL UNION ALL SELECT 'Spark', ROUND(AVG(EXTRACT(EPOCH FROM (updated_at - window_end)))::numeric, 3), ROUND(MIN(EXTRACT(EPOCH FROM (updated_at - window_end)))::numeric, 3), ROUND(MAX(EXTRACT(EPOCH FROM (updated_at - window_end)))::numeric, 3), COUNT(*) FROM spark_results WHERE updated_at IS NOT NULL;"
    @echo ""
    @echo "── 윈도우별 비교: Flink vs Spark vs 정답 vs Python ──"
    @docker compose exec postgres psql -U postgres -d streamdb \
        -c "SELECT COALESCE(f.window_start, e.window_start) AS window_start, COALESCE(f.flink_total, 0) AS \"Flink\", COALESCE(s.spark_total, 0) AS \"Spark\", COALESCE(e.event_total, 0) AS \"정답(Event Time)\", COALESCE(p.proc_total, 0) AS \"Python(Proc Time)\" FROM (SELECT window_start, SUM(total_amount) AS flink_total FROM flink_results GROUP BY window_start) f FULL OUTER JOIN (SELECT correct_window AS window_start, SUM(amount) AS event_total FROM event_log GROUP BY correct_window) e ON f.window_start = e.window_start FULL OUTER JOIN (SELECT assigned_window AS window_start, SUM(amount) AS proc_total FROM event_log GROUP BY assigned_window) p ON COALESCE(f.window_start, e.window_start) = p.window_start FULL OUTER JOIN (SELECT window_start, SUM(total_amount) AS spark_total FROM spark_results GROUP BY window_start) s ON COALESCE(f.window_start, e.window_start) = s.window_start ORDER BY window_start;"

# Spark 결과만 조회
query-spark:
    @docker compose exec postgres psql -U postgres -d streamdb \
        -c "SELECT window_start, window_end, user_id, total_amount, updated_at FROM spark_results ORDER BY window_start, user_id LIMIT 20;"

# ─────────────────────────────────────────────
# Fraud Detection: DataStream API 사기 탐지
# ─────────────────────────────────────────────

# Fraud Detection 실행: 인프라 + Flink + Fraud Generator
phase-fraud: clean infra _phase-fraud-start

# Fraud Detection 내부: 토픽 생성 → Flink Job 제출 → Fraud Generator 시작
_phase-fraud-start:
    #!/usr/bin/env bash
    set -euo pipefail
    echo ""
    echo "🔍 Fraud Detection: DataStream API 사기 탐지"
    echo ""
    # 1. Flink 클러스터 시작
    echo "⚙️  Flink 클러스터 시작..."
    docker compose up -d jobmanager taskmanager
    # 2. Kafka 토픽 생성
    echo "📡 Kafka 토픽(fraud-payments) 생성..."
    docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --create --topic fraud-payments \
        --partitions 1 --replication-factor 1 \
        --if-not-exists
    # 3. Flink 준비 대기
    echo "⏳ Flink 클러스터 준비 대기 (40초)..."
    sleep 40
    # 4. Fraud Detection Job 제출
    echo "🚀 Fraud Detection Job 제출..."
    docker compose exec -T jobmanager flink run -d -py /opt/flink/src/flink_fraud.py
    sleep 5
    # 5. Fraud Generator 시작
    echo "📡 Fraud Generator 시작..."
    docker compose up -d fraud-generator
    echo ""
    echo "✅ Fraud Detection 실행 중!"
    echo "   1~2분 후 just query-fraud 로 결과를 확인하세요."
    echo "   Flink Web UI: http://localhost:8081"

# Fraud Detection 결과 조회
query-fraud:
    @echo "── fraud_alerts (최근 20건) ──"
    @docker compose exec postgres psql -U postgres -d streamdb \
        -c "SELECT alert_id, user_id, small_amount, large_amount, alert_time FROM fraud_alerts ORDER BY alert_time DESC LIMIT 20;"
    @echo ""
    @echo "── 사용자별 사기 탐지 횟수 ──"
    @docker compose exec postgres psql -U postgres -d streamdb \
        -c "SELECT user_id, COUNT(*) as alert_count, AVG(large_amount) as avg_large_amount FROM fraud_alerts GROUP BY user_id ORDER BY alert_count DESC;"

# ─────────────────────────────────────────────
# 전체 정리
# ─────────────────────────────────────────────

# 모든 서비스 중지 (데이터 보존)
down:
    docker compose down

# 모든 서비스 + 볼륨 삭제 (완전 초기화)
destroy: clean

# 실행 상태 확인
status:
    @docker compose ps
    @echo ""
    @echo "── Flink Jobs ──"
    @docker compose exec -T jobmanager flink list 2>/dev/null || echo "ℹ️  Flink 클러스터가 실행 중이 아닙니다."

# 로그 확인 (서비스명 지정 가능, 기본: 전체)
logs *services:
    docker compose logs -f --tail=30 {{ services }}
