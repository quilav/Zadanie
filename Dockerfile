# Etap 1: Tworzymy tymczasowy obraz z zainstalowanym Scalą, sbt, Sparkiem i JDK
FROM sbtscala/scala-sbt:eclipse-temurin-focal-11.0.21_9_1.9.8_2.12.18 AS builder

# Instalujemy JDK
RUN apt-get update && apt-get install -y openjdk-8-jdk

# Pracujemy w katalogu /app
WORKDIR /app

# Kopiujemy pliki z katalogu src do katalogu /app/src w kontenerze
COPY src /app/src

# Kopiujemy build.sbt do katalogu /app w kontenerze
COPY build.sbt /app/build.sbt

# Kopiujemy pliki projektowe do katalogu /app w kontenerze
COPY . /app

# Budujemy projekt
RUN sbt clean package

# Etap 2: Drugi etap z docelowym obrazem Sparka
FROM apache/spark:3.3.3

# Pracujemy w katalogu /app
WORKDIR /app

# Kopiujemy zbudowany plik JAR z etapu budowy do /app
COPY --from=builder /app/target/scala-2.12/zadanie_2.12-0.1.0-SNAPSHOT.jar /app/Zadanie_2.12-0.1.0-SNAPSHOT.jar

# Kopiujemy plik danych CSV do katalogu /app w kontenerze
COPY src/main/resources/dane.csv /app/dane.csv

# Nadajemy odpowiednie uprawnienia dla plików
USER root
RUN chown -R spark:spark /app

# Przełączamy się z powrotem na użytkownika spark
USER spark

# Uruchamiamy nasz Spark job
CMD ["/opt/spark/bin/spark-submit", "--class", "TransformationJob", "--master", "local[*]", "/app/Zadanie_2.12-0.1.0-SNAPSHOT.jar"]
