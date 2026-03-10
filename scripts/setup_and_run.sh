#!/bin/bash
# Instala dependências e inicia o servidor
cd "$(dirname "$0")/.."
pip install -r requirements.txt
python run_server.py
