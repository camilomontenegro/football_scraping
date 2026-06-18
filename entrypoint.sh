#!/bin/sh

echo ">>> Configurando base de datos (tablas + competiciones)..."
python db/setup_db.py

echo ">>> Iniciando Streamlit..."
exec python -m streamlit run dashboard/app.py --server.address=0.0.0.0
