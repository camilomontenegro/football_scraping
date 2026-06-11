#!/bin/sh

echo ">>> Sembrando dim_competition..."
python loaders/competition_loader.py || echo ">>> Advertencia: seed de competiciones falló, continuando..."

echo ">>> Iniciando Streamlit..."
exec python -m streamlit run dashboard/app.py --server.address=0.0.0.0
