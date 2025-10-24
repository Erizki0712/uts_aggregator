
# Pub-Sub Log Aggregator (FastAPI, Python 3.11)

Aggregator ini menerima event/log via HTTP (`/publish`), memprosesnya secara idempoten (tidak memproses ulang event yang sama), serta melakukan deduplication berbasis `(topic, event_id)`.

---

## 1) Skema Event & Endpoint

### 1.1 Skema Event (JSON)
```json
{
  "topic": "string",
  "event_id": "string-unik",
  "timestamp": "ISO8601",
  "source": "string",
  "payload": { "bebas": "object" }
}
````

### 1.2 Daftar Endpoint

* `POST /publish`
  Terima single event atau batch (array).
  Respon: `{"accepted": <int>, "queued": <int>}`

* `GET /events?topic=<opt>`
  Mengembalikan daftar event unik yang telah diproses.

* `GET /stats`
  Contoh respon:

  ```json
  {
    "received": 6000,
    "unique_processed": 4800,
    "duplicate_dropped": 1200,
    "topics": ["alpha","beta"],
    "uptime_seconds": 123.456,
    "queue_depth": 0,
    "workers": 2,
    "db_path": "/data/store.db"
  }
  ```

* `GET /healthz`
  `{"ok": true}` untuk mengecek koneksi ke server.

### 1.3 Contoh Request

**Single (curl / Linux/macOS)**

```bash
curl -X POST http://localhost:8080/publish \
  -H "content-type: application/json" \
  -d '{"topic":"alpha","event_id":"e-1","timestamp":"2024-01-01T00:00:00Z","source":"manual","payload":{"x":1}}'
```

**Single (PowerShell)**

```powershell
$body = @{
  topic="alpha"; event_id="e-1"; timestamp="2024-01-01T00:00:00Z"; source="manual"; payload=@{x=1}
} | ConvertTo-Json -Depth 5
Invoke-RestMethod http://localhost:8080/publish -Method POST -ContentType "application/json" -Body $body
```

**Batch (curl)**

```bash
curl -X POST http://localhost:8080/publish \
  -H "content-type: application/json" \
  -d '[{"topic":"alpha","event_id":"b-1","timestamp":"2024-01-01T00:00:00Z","source":"manual","payload":{}},
       {"topic":"alpha","event_id":"b-1","timestamp":"2024-01-01T00:00:00Z","source":"manual","payload":{}}]'
```

---

## 2) Menjalankan Lokal (tanpa Docker)

> Pastikan Python 3.11, `pip`, dan `uvicorn` tersedia.

### 2.1 Setup & Run

```bash
# Linux/macOS
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
mkdir -p ./data
export DB_PATH="$(pwd)/data/store.db"
export CONSUMER_WORKERS=2
uvicorn src.main:app --host 0.0.0.0 --port 8080
```

```powershell
# Windows PowerShell
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
New-Item -ItemType Directory -Force .\data | Out-Null
$env:DB_PATH="$PWD\data\store.db"
$env:CONSUMER_WORKERS="2"
uvicorn src.main:app --host 0.0.0.0 --port 8080
```

> Catatan: DB & tabel dibuat otomatis saat start pertama. Ubah port lokal dengan `--port <angka>`.

---

## 3) Menjalankan dengan Docker Compose

### 3.1 Build & Run

```bash
docker compose build
docker compose up
# Akses: http://localhost:8080
```

### 3.3 Stop

```bash
docker compose down
docker volume rm aggregator_dedup_data
```

---

## 4) Unit Test (Lokal)

### 4.1 Menjalankan semua test

```bash
python -m pytest -q
```

---

## 5) Link video demo

https://youtu.be/l6pNOFgUCPY

---
