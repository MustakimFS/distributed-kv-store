FROM maven:3.9.6-eclipse-temurin-17 AS builder

WORKDIR /app

COPY pom.xml .
RUN mvn dependency:go-offline -q

COPY src ./src
RUN mvn clean package -DskipTests -q

FROM eclipse-temurin:17-jre-jammy

WORKDIR /app

COPY --from=builder /app/target/distributed-kv-store-1.0-SNAPSHOT.jar app.jar

EXPOSE 50051

ENTRYPOINT ["java", "-jar", "app.jar"]