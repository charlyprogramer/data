# Opciones de Yahoo Finance → BigQuery

Pipeline que descarga las cadenas de opciones de Yahoo Finance y las guarda en
BigQuery. Corre solo en la nube con GitHub Actions, de lunes a viernes a las
21:15 UTC (después del cierre del mercado de EE. UU.).

- **Tickers:** SOXL, SPXL, TQQQ, TECL, NVDL, AAPL
- **Qué guarda:** todos los vencimientos, calls y puts, con todos los campos
  que devuelve Yahoo por contrato y la cotización del subyacente en ese momento.
- **Destino:** `prueba-server-507503.yahoo_finance.options_chain`, particionada
  por `snapshot_date` y agrupada por `ticker`, `option_type` y `expiration`.

Cada corrida agrega una foto nueva (`snapshot_ts`); nunca borra datos anteriores.

## Columnas

| Columna | Descripción |
|---|---|
| `snapshot_ts`, `snapshot_date` | Momento (UTC) de la descarga |
| `ticker`, `option_type` | Subyacente y `call` / `put` |
| `expiration`, `days_to_expiration` | Vencimiento y días que faltan |
| `contract_symbol`, `strike`, `contract_size`, `currency` | Datos del contrato |
| `last_price`, `bid`, `ask`, `change`, `percent_change`, `last_trade_date` | Precios |
| `volume`, `open_interest`, `implied_volatility`, `in_the_money` | Actividad y volatilidad |
| `underlying_price`, `underlying_bid`, `underlying_ask`, `underlying_market_state` | Subyacente |
| `underlying_quote_json` | Cotización completa del subyacente, en JSON |

## Configuración (una sola vez)

1. **Cuenta de servicio en Google Cloud** (proyecto `prueba-server-507503`):
   IAM y administración → Cuentas de servicio → Crear. Dale los roles
   *Editor de datos de BigQuery* y *Usuario de trabajos de BigQuery*.
2. En esa cuenta, ve a **Claves → Agregar clave → JSON** y descarga el archivo.
3. En GitHub: repo → **Settings → Secrets and variables → Actions → New
   repository secret**. Nombre: `GCP_SA_KEY`; valor: el contenido completo del JSON.
4. En la pestaña **Actions** del repo, abre *Yahoo options to BigQuery* y pulsa
   **Run workflow** para probarlo sin esperar al horario.

## Cambiar tickers u horario

Edita `.github/workflows/yahoo-options-to-bigquery.yml`: la variable `TICKERS`
y la línea `cron` (en hora UTC).

## Correrlo en tu computadora

```bash
pip install -r requirements.txt
DRY_RUN=1 python options_to_bigquery.py   # solo genera un CSV, no toca BigQuery
python options_to_bigquery.py             # carga a BigQuery (requiere `gcloud auth application-default login`)
```

## Consulta de ejemplo

```sql
SELECT ticker, option_type, expiration, strike, bid, ask, implied_volatility, open_interest
FROM `prueba-server-507503.yahoo_finance.options_chain`
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM `prueba-server-507503.yahoo_finance.options_chain`)
  AND ticker = 'TQQQ'
ORDER BY expiration, option_type, strike;
```
