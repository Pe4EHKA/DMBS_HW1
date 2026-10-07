FROM maven:3.9.11-eclipse-temurin-21 AS sources
WORKDIR /workspace
COPY pom.xml .
RUN mvn -B dependency:go-offline
COPY src ./src
FROM sources AS tests
CMD ["mvn", "-B", "test"]
FROM sources AS build
RUN mvn -B -DskipTests package
FROM eclipse-temurin:21-jre-alpine AS runtime
WORKDIR /app
COPY --from=build /workspace/target/gamehub.jar app.jar
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]