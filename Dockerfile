FROM quay.io/quarkus/ubi9-quarkus-mandrel-builder-image:jdk-21 AS build
WORKDIR /build
COPY mvnw mvnw.cmd pom.xml ./
COPY .mvn .mvn
RUN ./mvnw dependency:go-offline -q
COPY src src
RUN ./mvnw clean package -Dnative -DskipTests -Dquarkus.native.container-build=false
FROM registry.access.redhat.com/ubi9/ubi-minimal:9.4
RUN microdnf install -y glibc-langpack-en && \
    microdnf clean all && \
    rm -rf /var/cache/yum
RUN useradd -r -u 1001 -g root exoquic
WORKDIR /app
COPY --from=build --chown=1001:root /build/target/*-runner /app/exoquic-agent
RUN chmod 755 /app/exoquic-agent
USER 1001
ENTRYPOINT ["/app/exoquic-agent"]