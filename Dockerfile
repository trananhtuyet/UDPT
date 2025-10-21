FROM eclipse-temurin:21-jdk

WORKDIR /app

COPY target/time-tracker-zookeeper-1.0-SNAPSHOT.jar app.jar
COPY Web /app/Web

EXPOSE 4567

CMD ["java", "-jar", "app.jar"]