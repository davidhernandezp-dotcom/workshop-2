# Guía paso a paso (Windows 11 + Docker Desktop; notas para Mac/Linux)

Tiempo estimado: 30–40 min la primera vez (la mayor parte es descargar/construir Docker).

## 0. Requisitos (una sola vez)
| Herramienta | Para qué | Comprobar |
|---|---|---|
| **Docker Desktop** (backend WSL2, ≥ 6 GB de RAM asignados) | Airflow + PostgreSQL | `docker --version` y `docker compose version` |
| **Git** (opcional) y **VS Code** | editar / versionar | `git --version` |
| **Python 3.12** (opcional) | tests y notebook en tu máquina | `py -3.12 --version` |
| **Power BI Desktop** (solo Windows) | dashboard | abrir la app |

> Docker Desktop debe estar **abierto y en estado "Running"** antes de seguir.
> Si usas Mac/Linux, en vez de Power BI usa Looker Studio, Tableau o Metabase (misma conexión: `localhost:5433`).

## 1. Preparar la carpeta
1. Descomprime `workshop-2.zip` (o clona tu repo). Abre **PowerShell** dentro de la carpeta `workshop-2`.
2. Verifica que existen `data/raw/spotify_dataset.csv` y `data/raw/the_grammy_awards.csv`.
3. Crea tu archivo de entorno:
   ```powershell
   Copy-Item .env.example .env
   ```
   *(Mac/Linux: `cp .env.example .env` y cambia `AIRFLOW_UID=50000` por el resultado de `id -u`.)*

## 2. Construir y arrancar
```powershell
docker compose build               # 3–6 min la primera vez: Airflow 3.1.8 + Great Expectations 1.23.1
docker compose up airflow-init     # espera: "airflow-init exited with code 0"
docker compose up -d
docker compose ps                  # todos "running" o "healthy" (airflow-init aparece "exited 0": es normal)
```
Servicios: Airflow UI `http://localhost:8080` (usuario **airflow** / clave **airflow**) y PostgreSQL analítico en `localhost:5433`.

## 3. Cargar la fuente relacional de Grammy (preparación de la fuente, NO es el ETL)
```powershell
docker compose exec airflow-scheduler python /opt/airflow/scripts/load_grammy_source.py
```
Debe terminar con `RECONCILED: True` (4810 filas en CSV = 4810 en la tabla `grammy_source.public.grammy_awards`).
Es idempotente: puedes repetirlo.

## 4. Prueba A — corrida exitosa
1. Abre `http://localhost:8080` → **Dags** → `grammy_spotify_reliable_pipeline` (si no aparece, espera 1 min o mira `docker compose logs airflow-dag-processor`).
2. Activa el interruptor (unpause) y pulsa **Trigger** (▶).
3. Abre la corrida → pestaña **Graph**: verás dos ramas paralelas (Spotify y Grammy) que se unen en `transform_and_integrate`. Debe quedar todo en verde (≈ 1 min).
4. Abre el log de `validate_grammys_raw`: verás `[FAIL] GR-08 warning ...` y la decisión `CONTINUE_WITH_WARNING`. **Es lo esperado** (la columna `winner` es True en el 100 % de las filas).
5. Verifica el DW:
   ```powershell
   docker compose exec etl-db psql -U etl_user -d music_dw -c "select * from dw.v_kpi_headline;"
   docker compose exec etl-db psql -U etl_user -d music_dw -c "select * from dw.v_kpi_popularity_by_decade;"
   ```
   Esperado: 4804 nominaciones, 304 emparejadas (6.33 %), popularidad media ≈ 59.7.
6. 📸 Evidencia: captura del Graph en verde, del log de una validación y de las consultas.

## 5. Prueba B — falla crítica controlada
```powershell
docker compose exec airflow-scheduler python /opt/airflow/scripts/make_bad_batches.py spotify
```
1. En Airflow: **Trigger** ▾ → **Trigger DAG w/ config** → pega `{"spotify_filename": "spotify_dataset_bad.csv"}` → Trigger.
2. Esperado: `validate_spotify_raw` en **rojo** (regla **SP-05**, 100 valores de popularidad = 150); `validate_grammys_raw` en verde; las 4 tareas siguientes en **upstream_failed** (naranja); el DW **no cambia**.
3. Comprueba que el DW sigue igual: repite la consulta del paso 4.5.
4. 📸 Captura el Graph, el log con `[FAIL] SP-05 critical` y el archivo `data/work/<run_id>/validation/spotify_raw.json`.
5. Recupérate: **Trigger** normal (sin config) → vuelve a verde.
6. *(Variante en la otra rama)* `make_bad_batches.py grammy` y config `{"grammy_table": "grammy_awards_bad"}` → falla **GR-03**.

## 6. Prueba C — rerun seguro y reintento
**Rerun:** lanza el DAG otra vez con la misma entrada. Luego:
```powershell
docker compose exec etl-db psql -U etl_user -d music_dw -c "select batch_key, load_count from dw.audit_load;"
docker compose exec etl-db psql -U etl_user -d music_dw -c "select count(*) as filas, count(distinct nomination_bk) as unicas from dw.fact_grammy_nomination;"
```
Esperado: `filas = unicas = 4804` y `load_count` aumenta en 1 (no se duplica nada).

**Reintento por falla transitoria:**
```powershell
docker compose stop etl-db      # simula la caída de la base fuente
```
Lanza el DAG: `extract_grammys` pasa a **up_for_retry** (hasta 3 reintentos; la espera crece con *back-off* exponencial: ≈20–40 s, 40–80 s, 80–120 s). Enciende la base de nuevo en cuanto veas el primer reintento (en menos de ~1 min):
```powershell
docker compose start etl-db
```
El siguiente intento debe terminar en éxito. 📸 Captura la pestaña **Details/Attempts** o el log con intento 1 fallido y el siguiente exitoso.
*Contraste:* config `{"grammy_table": "no_such_table"}` → `extract_grammys` falla **de inmediato, sin reintentos** (error determinístico).

## 7. Dashboard en Power BI
Sigue `docs/dashboard_guide.md`: Obtener datos → **PostgreSQL** → servidor `localhost:5433`, base `music_dw`, usuario `etl_user`, clave `etl_password` → selecciona las vistas `dw.v_kpi_*` → crea los 4 gráficos + 3 tarjetas. Referencia visual: `docs/evidence/dashboard_preview.png`.

## 8. (Opcional) Tests y notebook en tu máquina
```powershell
py -3.12 -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
# (los tests que usan la BD se conectan a localhost:5433 por defecto; si los contenedores están apagados, se omiten)
python -m pytest -q              # 32 tests (2 usan la BD)
python scripts/build_profiling_notebook.py   # regenera notebooks/data_profiling.ipynb
```

## 9. Apagar / reiniciar
```powershell
docker compose down        # apaga, conserva datos
docker compose down -v     # apaga y BORRA bases de datos (empezar de cero; repite el paso 3)
```

## Problemas frecuentes
| Síntoma | Causa / solución |
|---|---|
| `port is already allocated` (8080 o 5433) | otro programa usa el puerto: ciérralo o cambia el puerto izquierdo en `docker-compose.yaml` |
| El DAG no aparece | `docker compose logs airflow-dag-processor`; suele ser un error de importación |
| `ModuleNotFoundError: src` | falta `PYTHONPATH` → verifica que usas **este** `docker-compose.yaml` (lo define) |
| `etl-db` no crea las bases | el `.sh` quedó con saltos de línea de Windows (CRLF). Se arregla con `.gitattributes`; si descomprimiste a mano: en VS Code abre `sql/00_create_databases.sh` → barra inferior "CRLF" → **LF**, y luego `docker compose down -v && docker compose up -d` |
| `Permission denied` en `data/` o `logs/` (Linux/Mac) | `AIRFLOW_UID` debe ser tu `id -u` en `.env` |
| La tarea se queda sin memoria | sube la RAM de Docker Desktop (Settings → Resources) a ≥ 6 GB |
| `connection refused` al correr scripts fuera de Docker | usa `ETL_DB_HOST=localhost` y `ETL_DB_PORT=5433` |
