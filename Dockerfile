FROM eclipse-temurin:24-jre

WORKDIR /app

# Copy the fat jar — any version — and rename to app.jar
COPY target/*.jar app.jar

EXPOSE 8083

ENTRYPOINT ["java", "-jar", "/app/app.jar"]

