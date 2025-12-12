FROM eclipse-temurin:24-jre

WORKDIR /app

# Build-time version argument
ARG APP_VERSION

# Expose version as an image label (visible via docker ps / inspect)
LABEL org.opencontainers.image.version=$APP_VERSION

# Copy the fat jar — any version — and rename to app.jar
COPY target/*.jar app.jar

EXPOSE 8083

ENTRYPOINT ["java", "-jar", "/app/app.jar"]

