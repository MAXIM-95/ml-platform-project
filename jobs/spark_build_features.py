#импортирую spark 
#аналог import pandas as pd только для распределенных данных
from pyspark.sql import SparkSession #точка входа в спарк
from pyspark.sql import functions as F #набор SQL-функций (explode, to_date, avg…)


def main(): #оборачиваю в main чтобы файл можно было запускать как скрипт
    
    #создаю спарк-сессию. builder - конфигурация spark, appName - имя джобы, getOrCreate() - если сессия есть взять ее, иначе создать
    spark = SparkSession.builder.appName("build_features_vacancies_daily_skill").getOrCreate()

    # Важно: правильный HDFS внутри docker
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.defaultFS", "hdfs://namenode:8020")

    #пути в data lake (не трогаем raw, а строим витрину поверх)
    raw_path = "hdfs://namenode:8020/raw/vacancies/vacancies_raw.json" #сырые данные
    out_path = "hdfs://namenode:8020/features/vacancies_daily_skill" #витрина (фича слой)

    df = spark.read.option("multiLine", True).json(raw_path) #читаем json файл (multiline нужен, если json не однострочный)

    exploded = (
        df.withColumn("published_date", F.to_date("published_at")) #нормализую дату
          .withColumn("skill", F.explode("skills")) #распаковываю список вакансий
    )

    exploded.createOrReplaceTempView("vacancies_exploded") #создаю временную sql-таблицу, чтобы можно было писать SELECT

    #с помощью этого запроса строю витрину признаков (группирую по дате, региону, скиллам и считаю сколько вакансий, среднюю з/п)
    #в итоге получаем таблицу вида: | date | region | skill | count | avg_salary | (это датасет для ML)
    features = spark.sql("""
      SELECT
        published_date,
        region,
        skill,
        COUNT(*) AS vacancies_cnt,
        AVG(salary) AS avg_salary
      FROM vacancies_exploded
      GROUP BY published_date, region, skill
      ORDER BY published_date, region, skill
    """)

    #записываю результат в hdfs (overwrite перезаписывает витрину)
    features.write.mode("overwrite").parquet(out_path)
    spark.stop() #останавливаем спарк (job закончен, ресурсы освобождены)

#если файл запущен как скрипт → вызываем main()
#если импортирован как модуль → нет
if __name__ == "__main__":
    main()


#что делает этот скрипт:
#raw JSON
#   ↓
#explode skills
#   ↓
#SQL агрегаты
#   ↓
#parquet features

