import pandas as pd
from understatapi import UnderstatClient
import time
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.helpers import limpiar_y_transformar_tiros


# Inicializamos el cliente
client = UnderstatClient()

# --- CONFIGURACIÓN ---
LIGA = 'La_Liga'
TEMPORADAS = ['2020', '2021'] # Serviría solo la de 2020 que es la La_liga 2020/2021
EQUIPOS_OBJETIVO = ['Real Madrid', 'Barcelona']

# Creamos la carpeta de destino si no existe (pasos marcados por la guía)
ruta_carpeta = '../data/raw/'
if not os.path.exists(ruta_carpeta):
    os.makedirs(ruta_carpeta)

for temp in TEMPORADAS:
    print(f"\n--- Iniciando descarga: {LIGA} {temp} ---")
    
    try:
        # 1. Obtenemos los partidos de la temporada
        partidos = client.league(league=LIGA).get_match_data(season=temp)
    except Exception as e:
        print(f" Error al conectar con Understat para la temporada {temp}: {e}")
        continue

    todos_los_tiros = []

    # 2. Bucle de partidos
    for i, p in enumerate(partidos):
        # FILTRADO: Solo procesamos si juega Madrid o Barsa
        casa = p['h']['title']
        fuera = p['a']['title']
        
        if casa not in EQUIPOS_OBJETIVO and fuera not in EQUIPOS_OBJETIVO:
            continue

        match_id = p['id']
        print(f" > Procesando Match ID {match_id}: {casa} vs {fuera}")

        # Intento de descarga con reintentos
        intentos = 0
        exito = False
        while intentos < 3 and not exito:
            try:
                # Extraemos los disparos
                datos = client.match(match_id).get_shot_data()
                tiros_partido = datos['h'] + datos['a']
                
                # Inyectamos metadatos en cada tiro
                for tiro in tiros_partido:
                    tiro['match_id'] = match_id
                    tiro['h_team'] = casa
                    tiro['a_team'] = fuera
                    tiro['season'] = temp
                
                todos_los_tiros.extend(tiros_partido)
                exito = True
                # Respiro corto para no saturar la API (Apartado 1 de la guía)
                time.sleep(0.5) 
                
            except Exception as e:
                intentos += 1
                print(f"   Intento {intentos} fallido para partido {match_id}. Reintentando...")
                time.sleep(2) # Esperamos más tiempo si hay error

    # 3. Creación del DataFrame y Transformación
    if todos_los_tiros:
        df_temp = pd.DataFrame(todos_los_tiros)
        
        # LLAMADA A TU NUEVA FUNCIÓN DE LIMPIEZA
        df_temp = limpiar_y_transformar_tiros(df_temp)
        
        # 4. GUARDADO EN JSON
        nombre_archivo = f'shots_{LIGA}_{temp}.json'
        ruta_final = os.path.join(ruta_carpeta, nombre_archivo)
        
        df_temp.to_json(ruta_final, orient='records', indent=4, force_ascii=False)

    print(f" Temporada {temp} finalizada: {len(df_temp)} tiros guardados.")