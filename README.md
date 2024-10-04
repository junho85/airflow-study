# airflow-study

## run airflow
```shell
AIRFLOW_HOME=~/airflow_study airflow standalone 
```
database 초기화, 유저 생성, 모든 컴포넌트 시작



## airflow init
standalone 으로 실행했으면 init을 하지 않아도 된다.
```shell
AIRFLOW_HOME=~/airflow_study airflow db migrate
```

8080 포트를 8090
8793 포트를 8703
8794 포트를 8704
