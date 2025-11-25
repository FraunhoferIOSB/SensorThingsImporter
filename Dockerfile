FROM eclipse-temurin:21-alpine

# Copy to images tomcat path
ARG JAR_FILE
COPY target/${JAR_FILE} /usr/local/FROST/FROST-Importer.jar
WORKDIR /usr/local/FROST
CMD ["java", "-XX:+UseContainerSupport", "-XX:MaxRAMPercentage=80", "-jar", "FROST-Importer.jar", "-s", "-c", "config/scheduler.json"]
