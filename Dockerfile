# ==========================================
# [Stage 1] Builder: PyFlink 컴파일 환경
# ==========================================
# 왜: 실습 목표에 맞춰 최신 Flink 런타임을 기준으로 비교한다.
FROM flink:latest AS builder

USER root

# 빌드용 도구 설치 (JDK, GCC, Python Dev)
RUN apt-get update && apt-get install -y \
    python3 \
    python3-dev \
    python3-venv \
    build-essential \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# uv (빠른 Python 패키지 매니저)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# 가상환경 생성 후 PyFlink 설치
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN JAVA_HOME=/usr/lib/jvm/default-java \
    uv pip install apache-flink

# ==========================================
# [Stage 2] Runner: 실행 전용 경량 이미지
# ==========================================
FROM flink:latest

USER root

# Python 런타임 + wget (JAR 다운로드용)
RUN apt-get update && apt-get install -y \
    python3 wget \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

# Builder에서 조립된 가상환경 복사
COPY --from=builder /opt/venv /opt/venv

ENV VIRTUAL_ENV=/opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# -----------------------------------------------
# Flink Connector JAR 다운로드
# 빌드 실패 시 https://repo1.maven.org/maven2/org/apache/flink/ 에서 최신 버전 확인
# -----------------------------------------------
ARG KAFKA_CONNECTOR=4.0.0-2.0
ARG JDBC_CORE_CONNECTOR=4.0.0-2.0
ARG JDBC_POSTGRES_CONNECTOR=4.0.0-2.0
ARG POSTGRES_DRIVER=42.7.4

RUN wget -q -P /opt/flink/lib/ \
        "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR}/flink-sql-connector-kafka-${KAFKA_CONNECTOR}.jar" \
    && wget -q -P /opt/flink/lib/ \
        "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc-core/${JDBC_CORE_CONNECTOR}/flink-connector-jdbc-core-${JDBC_CORE_CONNECTOR}.jar" \
    && wget -q -P /opt/flink/lib/ \
        "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc-postgres/${JDBC_POSTGRES_CONNECTOR}/flink-connector-jdbc-postgres-${JDBC_POSTGRES_CONNECTOR}.jar" \
    && wget -q -P /opt/flink/lib/ \
        "https://jdbc.postgresql.org/download/postgresql-${POSTGRES_DRIVER}.jar"

# JRE에 없는 jdk.compiler 모듈 참조를 제거하여 WARNING 억제
RUN sed -i 's/ --add-exports=jdk.compiler\/[^ ]*//g' /opt/flink/conf/config.yaml

USER flink
WORKDIR /opt/flink
