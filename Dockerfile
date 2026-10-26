# syntax=docker/dockerfile:1
FROM eclipse-temurin:21.0.8_9-jdk@sha256:7d1d666ddafac14da0ded6b4b076becf76cf88b31f9d7953a76555cc82f86511 AS build
WORKDIR /src
COPY gradlew settings.gradle.kts build.gradle.kts gradle.lockfile ./
COPY gradle gradle
RUN ./gradlew --no-daemon -q dependencies > /dev/null
COPY src/main src/main
RUN ./gradlew --no-daemon -q jar \
 && cp build/libs/keel-0.1.0.jar /keel.jar \
 && mkdir /data

FROM gcr.io/distroless/java21-debian12:nonroot@sha256:7e37784d94dccbf5ccb195c73b295f5ad00cd266512dfbac12eb9c3c28f8077d
COPY --from=build /keel.jar /app/keel.jar
COPY --from=build --chown=65532:65532 /data /data
USER nonroot
EXPOSE 7000 8080
VOLUME /data
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-XX:+ExitOnOutOfMemoryError", "-jar", "/app/keel.jar"]
CMD ["server"]
