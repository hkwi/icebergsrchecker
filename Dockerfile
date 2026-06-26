FROM maven:3.9.15-eclipse-temurin-26 AS build

WORKDIR /workspace

ARG http_proxy
ARG https_proxy
ARG HTTP_PROXY
ARG HTTPS_PROXY

ENV http_proxy=${http_proxy}
ENV https_proxy=${https_proxy}
ENV HTTP_PROXY=${HTTP_PROXY}
ENV HTTPS_PROXY=${HTTPS_PROXY}

COPY pom.xml .
COPY .mvn .mvn
COPY src src

RUN mvn -ntp -DskipTests package


FROM eclipse-temurin:26-jre AS runtime

WORKDIR /app

ARG http_proxy
ARG https_proxy
ARG HTTP_PROXY
ARG HTTPS_PROXY

ENV http_proxy=${http_proxy}
ENV https_proxy=${https_proxy}
ENV HTTP_PROXY=${HTTP_PROXY}
ENV HTTPS_PROXY=${HTTPS_PROXY}

COPY --from=build /workspace/target/iceberg-sr-checker-1.0.0.jar /app/app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app/app.jar"]