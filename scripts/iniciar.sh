#!/bin/bash
# Inicia o servidor - usa .venv_packages se venv não existir
cd "$(dirname "$0")/.."

if [ -d "venv/bin" ]; then
  source venv/bin/activate
  exec python run_server.py
else
  export PYTHONPATH=".venv_packages:."
  exec python3 run_server.py
fi
