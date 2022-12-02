FROM maven:3.8-jdk-8-slim AS builder

COPY . /flink-job
WORKDIR /flink-job
RUN mvn clean install

FROM bde2020/flink-base:1.14.5-hadoop3.2

COPY ./worker.sh /
COPY ./lib/*.jar /
COPY ./conf /
COPY --from=builder /flink-job/target/flink-hudi-demo-*.jar /flink-hudi-demo.jar

EXPOSE 8081

CMD ["/bin/bash", "/worker.sh"]
