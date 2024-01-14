# Zadanie rekrutacyjne

## Opis Projektu

Projekt ma na celu transformację danych za pomocą Apache Spark i Scali. Napisana aplikacja pobiera dane
wejściowe [[dane.csv](src/main/resources/dane.csv)], a następnie dokonuje transformacji tak, aby finalnie otrzymać
agregat z informacjami o maksymalnie pięciu ostatnich użyciach przeglądarek internetowych, wraz z datami i id ich
użycia, dla danych krajów. Wynik to JSON o
strukturze:

```json
{
  "Ukraine": [
    {
      "Chrome": [
        {
          "id1": "date1",
          "id2": "date2"
        }
      ],
      "Safari": [
        {
          "id3": "date3"
        }
      ]
    }
  ],
  "USA": [],
  ...
}
```

Wyniki transformacji zamieszczono w pliku [wyniki.json](src/main/resources/wyniki.json).

## Struktura Projektu

```
|-- root
|-- .gitignore
|-- build.sbt
|-- Dockerfile
|-- README.md
|-- src/
| |-- main/
| |-- scala/
| | |-- TransformationJob.scala # Główna klasa przetwarzająca dane
| |-- resources/
| | |-- dane.csv # Plik danych wejściowych w formacie CSV
| | |-- wyniki.json # Podglądowe wyniki aplikacji
```

## Instrukcje Użytkowania

**Budowa obrazu Docker:**

   ```bash
   docker build -t nazwa_obrazu . 
   ``` 

Należy zbudować obraz Docker na podstawie pliku Dockerfile i nazwać go go zgodnie z preferencjami, np. nazwa_obrazu.

**Uruchamianie kontenera:**

   ```bash
   docker run -it nazwa_obrazu
   ```

Należy uruchomić kontener na podstawie zbudowanego obrazu. Proces przetwarzania danych rozpocznie się automatycznie w
kontenerze. W poleceniu uwzględniono sleep, aby użytkownik miał czas na skopiowanie pliku.
Krok ten można również wykonać inaczej, uruchamiając kontener i bezpośrednio z /bin/bash uruchomić komendę samemu.
Wersja ta daje użytkownikowi nieco więcej zabawy.

   ```bash
   docker run -it nazwa_obrazu
   ```

A następnie:

   ```bash
  /opt/spark/bin/spark-submit --class TransformationJob --master local[*] /app/zadanie_2.12-0.1.0-SNAPSHOT.jar
   ```

**Skopiowanie pliku:**

   ```bash
  docker cp nazwa_kontenera:/app/wyniki /ścieżka/lokalna
   ```

W innym terminalu wyniki powinno się skopiować z kontenera na lokalny dysk. Zamiast nazwa_kontenera, trzeba wstawić
rzeczywistą nazwę kontenera (można sprawdzić ją za pomocą polecenia docker ps -a). W repozytorium również zamieszczone
zostały [wyniki](src/main/resources/wyniki.json).

**Zatrzymanie i usunięcie kontenera i obrazu (opcjonalne):**

   ```bash
    docker stop nazwa_kontenera
    docker rm nazwa_kontenera
    docker rmi nazwa_obrazu
   ```

Warto jest po sobie sprzątać, w związku z czym dla chętnych proponowane jest zatrzymanie, a następnie usunięcie obrazu i
kontenera.

## Notki na koniec

- Pozwoliłem sobie usunąć z końcowego agregatu wyniki dla kraju "(not set)". Można by to obsłużyć inaczej (filter, map,
  obsługa wartości niestandardowych i pewnie jeszcze kilka innych). Wyszedłem z założenia, że inżynier danych nie ma
  problemu, żeby zajrzeć w dane.
- Dziwny ten końcowy JSON. Zdecydowanie bardziej wolałbym zapisać te wartości bez pivotu tablicy i dla innej struktury, w ten sposób późniejszy odczyt byłby prostszy. Struktura JSON, o której mówię:

```json
{
  "country": "Russia",
  "browser_data": [
    {
      "browser": "Chrome",
      "id_and_date_list": [
        {
          "fullVisitorId": "0168889143060863679",
          "date": "20170922"
        },
        {
          "fullVisitorId": "0885964130554821519",
          "date": "20180408"
        }
      ]
    },
    {
      "browser": "Firefox",
      "id_and_date_list": [
        {
          "fullVisitorId": "0032646285800256877",
          "date": "20180416"
        },
        {
          "fullVisitorId": "717573771141372604",
          "date": "20180218"
        }
      ]
    }
  ]
}
```