# Genie Bot (Genie + Streamlit)

Streamlit app to interact with Databricks Genie Spaces.

---

## 📦 Requirements

- Python 3.13 (recommended)
- pip (updated)
- Access to a Databricks' Genie Space

---

## ⚙️ Installation

Install dependencies:

```bash
pip install -r requirements.txt
```

## 🔑 Configure credentials

Create .env file and set real values for local testing:

GENIE_SPACE="xxxxxx"\
DATABRICKS_HOST="xxx-xxxxxxxx-xxxx.cloud.databricks.com"\
DATABRICKS_TOKEN="dapixxxxxx"

## 🧪 Run tests

```bash
pytest -q
```

## ▶️ Run-time

### Option 1: Run Genie Bot

 Run Genie Bot
```bash
streamlit run genie_bot.py
```

### Option 2: Run Genie Bot using Docker image

build image
```bash
docker build -t genie_app_image .
```

run the container, mapping host port 8501 to container 8501
```bash
docker run --rm -p 8501:8501 --name genie_app genie_app_image
```
## 🛑 Kill server

### Option 1:

(on terminal) Ctrl+C

### Option 2:

Open Task Manager (Ctrl+Alt+Supr), look for a process named 'python.exe' or 'streamlit.exe', right click and 'end task'

### Option 3:

(on terminal)
tasklist | findstr python
taskkill /PID <your_pid_number> /F