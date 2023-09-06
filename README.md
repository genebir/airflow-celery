<h1>AIRFLOW</h1>
<h2>Celery Executor Example</h2>
<br/>

<h3>Set path & config [기본 Path 지정 및 필수 폴더 생성]</h3>
docker compose file에서 호출하는 변수와 volume을 생성하는 폴더

```bash
export AIRFLOW_PROJ_DIR=<your_path>

mkdir -p $AIRFLOW_PROJ_DIR/dags
mkdir -p $AIRFLOW_PROJ_DIR/logs
mkdir -p $AIRFLOW_PROJ_DIR/plugins
mkdir -p $AIRFLOW_PROJ_DIR/config
```

<h3>Set Fernet Key [Fernet Key 생성]</h3>

```bash
export AIRFLOW__CORE__FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")
```