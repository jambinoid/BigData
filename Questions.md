## Вопросы
1. Большие данные и `NoSQL`. Основные причины возникновения явления. 
2. "Теорема" `CAP`
3. Экосистема `Hadoop`, Устройство `HDFS`.
4. Модель памяти `JVM`, основные идеи. Отношение `happens-before`. Ключевое слово `volatile`, 
5. Примитивы синхронизации в `JVM` (`java.util.concurrent`)
6. [Модель акторов](https://en.wikipedia.org/wiki/Actor_model), основные идеи, достоинства и недостатки. Примеры на `Akka`.
7. Работа с потоками и процессами из `Python`. `GIL`. Модуль `multiprocessing`, способы реализации (spawn, fork, forkserver)
8. `Apache Spark` - общая архитектура. Работа с `HDFS`, data locality.
9. Классический подход с `RDD`, достоинства и недостатки. 
10. `Dataset API`, `Spark SQL` & `DataFrames` - мотивация, основные идеи. 


## Литература:
1. https://assets.bitbashing.io/papers/concurrency-primer.pdf (Q4, Q5)
2. https://github.com/heathermiller/dist-prog-book (Q1, Q2, Q3, Q6)
3. https://docs.oracle.com/javase/specs/jls/se7/html/jls-17.html (Q4, Q5)
4. https://docs.python.org/3/library/multiprocessing.html (Q7)
5. https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html (Q1, Q3)
6. http://greenteapress.com/semaphores/LittleBookOfSemaphores.pdf (Q4, Q5)
7. https://spark.apache.org/docs/2.3.0/ (Q8, Q9, Q10)
8. https://akka.io/docs/ (Q6)
9. Холден Карау,  «Энди Конвински, Патрик Венделл, Матей Захария, Изучаем Spark. Молниеносный анализ данных»,  ДМК Пресс, 2015 (Q8, Q9)

Data:
https://drive.google.com/open?id=1BqHzpcPg72yUJMpTzIEdQUs-TR2oIWaw