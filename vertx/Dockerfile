#FROM vertx/vertx3
FROM openjdk:8-jre-alpine
#ENV VERTICLE_NAME io.vertx.vertx.MainVerticle
ENV VERTICLE_FILE vertx-1.0.0-SNAPSHOT-fat.jar
ENV VERTICLE_HOME /user/verticles
ENV MYSQL_HOST db-3.cpxfttks2x6x.us-east-1.rds.amazonaws.com
ENV MYSQL_NAME root
ENV MYSQL_PWD ilovepwd
EXPOSE 80
EXPOSE 3306
COPY target/$VERTICLE_FILE $VERTICLE_HOME/
WORKDIR $VERTICLE_HOME
ENTRYPOINT ["sh", "-c"]
#CMD ["exec vertx run $VERTICLE_NAME -cp $VERTICLE_HOME/*"]
CMD ["exec java -jar $VERTICLE_FILE"]
