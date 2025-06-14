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
WORKDIR /app
COPY --from=build /build/target/*-runner /app/exoquic-agent
RUN chmod 755 /app/exoquic-agent
ENTRYPOINT ["/app/exoquic-agent"]