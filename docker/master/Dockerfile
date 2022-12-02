FROM bde2020/flink-base:1.14.5-hadoop3.2

COPY ./conf /
COPY ./master.sh /
COPY ./lib/*.jar /
ENV FLINK_MASTER_LOG /usr/local/flink/log

EXPOSE 6123 22 8080

CMD ["/bin/bash", "/master.sh"]
