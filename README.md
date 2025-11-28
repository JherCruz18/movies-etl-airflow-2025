# ETL Pipeline - Películas Dataset

## 📋 Descripción
Pipeline ETL implementado con Apache Airflow para procesar y transformar el dataset de películas "Latest 2025 movies Datasets". El pipeline sigue la arquitectura medallion (Bronze → Silver → Gold) para garantizar calidad y trazabilidad de los datos.

## ✅ Resumen de lo Realizado

### Refactorización y Mejora de Estructura
- ✅ **Centralización de configuración**: Creación de `config.py` con todas las constantes (rutas, logging)
- ✅ **Modularización de scripts**: Actualización de `extract_bronze.py`, `extract_silver.py` y `extract_gold.py` para importar desde `config.py`
- ✅ **Eliminación de duplicidad**: Removidos hardcoded paths de cada script
- ✅ **Correcta gestión de imports**: Scripts en `dags/scripts/` importan correctamente desde `dags/config.py`

### Optimización del DAG
- ✅ **Actualización a Airflow 2.x+**: Cambio de `schedule_interval` a `schedule`
- ✅ **Removido parámetro deprecado**: Eliminado `provide_context=True` incompatible con versión actual
- ✅ **Mejor naming**: Nombres descriptivos en task_ids (`extract_bronze`, `transform_silver`, `load_gold`)
- ✅ **Default args centralizados**: Configuración común para todas las tareas
- ✅ **Metadata mejorada**: Agregados descripción y tags al DAG

### Validación y Ejecución
- ✅ **DAG funcional**: Pipeline ejecutándose correctamente en Docker Compose
- ✅ **Tres etapas trabajando**: Bronze → Silver → Gold en secuencia

### Documentación y Versionamiento
- ✅ **README completo**: Documentación detallada del proyecto
- ✅ **Repositorio Git**: Proyecto subido a GitHub con `.gitignore`
- ✅ **Commits estructurados**: Historial claro de cambios

## 🏗️ Estructura del Proyecto

```
Entregable-3/
├── dags/
│   ├── etl-peliculas-dag.py      # DAG principal de Airflow
│   ├── config.py                 # Configuración centralizada
│   └── scripts/
│       ├── extract_bronze.py     # Extracción de datos
│       ├── extract_silver.py     # Transformación y limpieza
│       └── extract_gold.py       # Carga de métricas de negocio
├── data/
│   ├── origin/                   # Datos originales (CSV)
│   ├── bronze/                   # Capa Bronze (datos sin procesar)
│   ├── silver/                   # Capa Silver (datos limpios)
│   └── gold/                     # Capa Gold (datos finales)
├── logs/                         # Logs de ejecución de Airflow
├── docker-compose.yaml           # Configuración de Docker
├── config/
│   └── airflow.cfg              # Configuración de Airflow
└── README.md                     # Este archivo
```

## 🚀 Inicio Rápido

### Requisitos
- Docker y Docker Compose instalados
- Python 3.8+ (para desarrollo local)

### Instalación y Ejecución

1. **Clonar o descargar el repositorio**
```bash
git clone https://github.com/TU_USUARIO/Entregable-3.git
cd Entregable-3
```

2. **Iniciar Airflow con Docker**
```powershell
docker-compose up -d
```

3. **Acceder a Airflow UI**
```
http://localhost:8080
```

4. **Activar el DAG**
   - En la UI de Airflow, busca `cem_pipeline`
   - Haz clic en el toggle para activarlo
   - Haz clic en "Trigger DAG" para ejecutarlo manualmente

### Detener Airflow
```powershell
docker-compose down
```

## 📊 Pipeline DAG: `cem_pipeline`

El DAG ejecuta tres tareas en secuencia:

### 1. **extract_bronze** 
- Lee el CSV original: `Latest 2025 movies Datasets.csv`
- Valida que el archivo exista
- Limpia espacios en blanco
- Guarda en `data/bronze/bronze_data.csv`

### 2. **transform_silver**
- Lee datos desde Bronze
- Limpia datos faltantes
- Normaliza tipos de datos
- Elimina duplicados
- Valida rangos de valores
- Guarda en `data/silver/silver_data.csv`

### 3. **load_gold**
- Lee datos desde Silver
- Crea nuevas columnas de negocio (año, mes, categorías)
- Genera estadísticas por idioma y año
- Clasifica películas por calificación y popularidad
- Guarda en `data/gold/gold_data.csv`

## ⚙️ Configuración

### Rutas de Datos (config.py)
```python
ORIGIN_PATH = '/opt/airflow/data/origin/Latest 2025 movies Datasets.csv'
BRONZE_PATH = '/opt/airflow/data/bronze/bronze_data.csv'
SILVER_PATH = '/opt/airflow/data/silver/silver_data.csv'
GOLD_PATH = '/opt/airflow/data/gold/gold_data.csv'
```

### Programación
- **Frecuencia**: Diariamente (`@daily`)
- **Hora**: A las 00:00 UTC (configurable en Airflow)
- **Reintentos**: 1 intento fallido = reintentar en 5 minutos

## 📝 Variables de Entorno

Si necesitas cambiar configuraciones, edita `config.py`:

```python
# Logging
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
TIMESTAMP = datetime.now().isoformat()
```

## 🔧 Desarrollo Local (sin Docker)

Si prefieres ejecutar localmente:

```powershell
# Activar entorno virtual
.venv\Scripts\Activate.ps1

# Instalar dependencias
pip install -r requirements.txt

# Inicializar Airflow
airflow db init

# Crear usuario
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com

# Iniciar webserver (en una terminal)
airflow webserver --port 8080

# Iniciar scheduler (en otra terminal)
airflow scheduler
```

## 📈 Monitoreo

En la UI de Airflow puedes:
- Ver el estado de las ejecuciones
- Consultar logs de cada tarea
- Monitorear tiempos de ejecución
- Reejecutar tareas fallidas

## 🐛 Troubleshooting

### Error: "DAG not found"
- Verifica que `etl-peliculas-dag.py` esté en la carpeta `dags/`
- Reinicia los contenedores: `docker-compose restart`

### Error: "File not found"
- Verifica que el archivo CSV está en `data/origin/`
- Comprueba las rutas en `config.py`

### Error: "Import error"
- Verifica que todos los scripts están en `dags/scripts/`
- Comprueba que `config.py` está en `dags/`

## 📚 Recursos

- [Apache Airflow Docs](https://airflow.apache.org/docs/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Docker Documentation](https://docs.docker.com/)

## 👤 Autor
Jhersson Cruz - Ingeniero de Sistemas

## 📄 Licencia
MIT
