FROM openjdk:17-jdk-slim

WORKDIR /app

COPY target/*.jar cicd.jar

EXPOSE 8080

CMD ["java", "-jar", "cicd.jar"]
