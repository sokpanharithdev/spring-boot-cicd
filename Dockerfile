FROM openjdk:17-jdk-slim

WORKDIR /app

ARG APPLICATION_NAME=cicd.jar

COPY target/*.jar ${APPLICATION_NAME}

EXPOSE 8080

CMD["java", "-jar", "${APPLICATION_NAME}"]
