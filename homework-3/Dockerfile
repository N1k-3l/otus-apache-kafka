FROM maven:3.9-eclipse-temurin-11 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package -DskipTests

FROM eclipse-temurin:11-jre
WORKDIR /app
COPY --from=build /build/target/app.jar ./app.jar
COPY --from=build /build/target/libs ./libs
ENTRYPOINT ["java", "-cp", "app.jar:libs/*"]
