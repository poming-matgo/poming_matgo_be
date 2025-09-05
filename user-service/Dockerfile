FROM gradle:7.6.0-jdk17 AS builder
ENV SPRING_PROFILES_ACTIVE=prod
WORKDIR /app
COPY . .
RUN gradle build -x test --no-daemon

FROM openjdk:17-jdk-alpine
ARG JAR_FILE=build/libs/*.jar
COPY --from=builder /app/${JAR_FILE} app.jar

EXPOSE 8082
ENTRYPOINT ["java", "-Dspring.profiles.active=prod", "-jar", "/app.jar"]