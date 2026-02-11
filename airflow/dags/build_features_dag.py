#Нужен, чтобы задать дату начала DAG (start_date). Airflow требует start_date для планирования
from datetime import datetime 
#Импортирует класс DAG — “контейнер” для задач
from airflow import DAG 
#Оператор, который выполняет shell-команду. Тут мы будем запускать spark-submit командой
from airflow.operators.bash import BashOperator

with DAG( #контекстный менеджер
    dag_id="build_features_vacancies_daily_skill", #Уникальное имя DAG’а. Именно оно будет отображаться в Airflow UI
    start_date=datetime(2026, 2, 1), #Airflow считает, что DAG “существует” начиная с этой даты (это не значит “запустить 1 февраля”, это просто опорная дата для расписания)
    schedule=None,   # запускаем вручную кнопкой (schedule=None означает, что не запускать по расписанию)
    catchup=False, #говорит "Не догоняй прошлое, запускай только новые"
    tags=["ml-platform", "spark", "features"], #теги для удобной группировки в UI. Не обязательны, но удобно
) as dag: #закрываю описание DAG и вхожу в его тело

    build_features = BashOperator( #build_features — объект задачи (cоздаём задачу (task) типа “выполни bash-команду”)
        task_id="build_features", #уникальный ID задачи внутри DAG’а. В UI будет видно именно это имя
        #далее команды для airflow
        bash_command=( 
            "docker exec spark-master " #выполняем команду внутри контейнера spark-master
            "/opt/spark/bin/spark-submit " #запускаем Spark job в нормальном режиме “spark-submit”
            "--master spark://spark-master:7077 " #говорим spark-submit: “используй Spark Standalone master”
            "/opt/airflow/jobs/spark_build_features.py" #путь к нашему скрипту в контейнере airflow (мы его монтируем volume’ом)
        ),
    )
   #Эта строка не обязательна — она просто оставляет задачу в DAG.
   #Если задач несколько, тут обычно пишут зависимости: task1 >> task2
    build_features

#ЧТО ДЕЛАЕТ ЭТОТ DAG: Airflow запускает Spark job по кнопке, чтобы пересобрать витрину features в HDFS