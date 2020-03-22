FROM openjdk:11-jre

# Copy to images tomcat path
ARG JAR_FILE
COPY target/${JAR_FILE} /usr/local/FROST/FROST-Importer.jar
WORKDIR /usr/local/FROST
CMD ["java", "-XX:+UnlockExperimentalVMOptions", "-XX:+UseCGroupMemoryLimitForHeap", "-jar", "FROST-Importer.jar", "-s", "-c", "config/scheduler.json"]
