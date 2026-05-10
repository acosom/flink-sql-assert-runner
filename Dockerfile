FROM eclipse-temurin:11-jre

WORKDIR /app

COPY target/flink-sql-assert-runner.jar /app/flink-sql-assert-runner.jar
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
