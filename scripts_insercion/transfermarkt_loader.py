import sys
import os
## Añade la carpeta raíz del proyecto al path para que los imports funcionen
# independientemente de desde dónde se ejecute el script
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import unicodedata
import pandas as pd
from db.models import session_scope, DimPlayer, FactInjuries
from utils.player_matcher import resolve_player


# encuentra la carpeta raiz 
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ══════════════════════════════════════════════════
# FUNCIONES AUXILIARES 
# ══════════════════════════════════════════════════
def normalize(name):
    """Quita tildes y convierte a minúsculas para comparar nombres."""
    return unicodedata.normalize('NFD', name)\
        .encode('ascii', 'ignore')\
        .decode('utf-8')\
        .lower()

def safe_date(val):
    """ 
    Maneja la conversión del formato vigente en el DataFrame al formato exigido por el campo  en fact_injuries.

    Evita excepción si en alguna fila la fecha es nula o tiene formato distinto.
    """
    try:
        return pd.to_datetime(val, format="%d/%m/%Y").date()
    except:
        return None


def map_positions (df_players):
    """
    Normaliza la columna position del DataFrame de Transfermarkt
    al formato de dim_player (G, D, M, F).
    """

    POSITION_MAP = {
        "Portero": "G",
        "Defensa central": "D",
        "Lateral derecho": "D",
        "Lateral izquierdo": "D",
        "Pivote": "M",
        "Mediocentro": "M",
        "Mediocentro ofensivo": "M",
        "Extremo derecho": "M",
        "Extremo izquierdo": "M",
        "Mediapunta": "M",
        "Delantero centro": "F",
    }
    df_players["position"] = df_players["position"].map(POSITION_MAP)
    return df_players


# ══════════════════════════════════════════════════
#  AÑADE CAMPOS AL DATAFRAME DF_PLAYERS PRESENTES EN   DIM_PLAYERS 
# ══════════════════════════════════════════════════

def enrich_players_df(df_players, session):
    """
    Añade birth_date y nationality al DataFrame  de jugadores consultando  la tabla dim_player por nombre.

    Recive un DataFrame con datos de jugadores extraidos de Transfermarkt 

    Si se quiere utilizar la funcion resolver_player de player_matcher, como esta función utiliza campos de la tabla dim_players  que no estan en el csv de jugadores, hay que cargar esos campos en el Dataframe. 
    Si no los cargamos, lo que ocurrira al utilziar revolve_player es que no se itentifca una equivalencia , el score que se otorga es bajo y la consecuencia es que se crean  registros solo  con el campo  de id de transfermarkt. 
    """
    ##obtiene los judagores de la tabla 
    candidates = session.query(DimPlayer).all()

    birth_dates = []
    nationalities = []
    
    # recorre la columna del DataFrame y comprueba si el nombre del jugador en el DataFrame es igual el nombre que existe en la tabla. A efectos de la comparación, se normaliza el nombre que viene de la tabla 

    for _, name in df_players["player_name"].items():
        #  next devuelve el primer elemento de un iterable que cumpla una condición, o un valor por defecto si no encuentra ninguno.
        # match guarda temporalmente el objeto dim_player 
        match = next(
            (candidate for candidate in candidates if normalize(candidate.name_canonical) == normalize(name)),
            None
        )
        birth_dates.append(match.birth_date if match else None)
        nationalities.append(match.nationality if match else None)


    df_players["birth_date"] = birth_dates
    df_players["nationality"] = nationalities

    return df_players


# ══════════════════════════════════════════════════
#  ACTUALIZA EL CAMPO id_transfermarkt en dim_players
# ══════════════════════════════════════════════════

def update_transfermarkt_ids_in_dim_players(df_players, session):
    """
    Actualiza el campo id_transfermarkt en dim_player para cada jugador del DataFrame.
    """
    # Por cada fila del DataFrame, construye el diccionario incoming con el nombre y el ID de Transfermarkt, la fecha de nacimiento y la nacionalidad
    for _, row in df_players[["player_name", "player_id", "birth_date", "nationality","position"]].iterrows():
        incoming = {
            "name": row["player_name"],
            "source_id": row["player_id"],
            "source_system": "transfermarkt",
            "birth_date": row["birth_date"],
            "nationality": row["nationality"],
            "position": row["position"],
        }
        # Se llama a resolve_player para que consulte la tabla de jugadores y compare el nombre del jugador con el nombre que hay en el csv 
        # resolve_player asigna score y decide en funcion del score (match automático, revision manual o crea nuevo jugador)
        try:
            with session.begin_nested():
                resolve_player(incoming, session, id_field="id_transfermarkt")
        except Exception as e: 
            print(f'[ERROR] player={row["player_name"]} | {e}')
            continue
    print("Jugadores cargados")


### Problema: hay jugadores en los que en la tabla dim_player el nombre no esta igual  que en el csv  y no se  puede solucionar con la funcion  normalice. Se trata de casos como el de 'Nacho' en dim player y 'Nacho fernandez' en el csv o Dani Carvajal en  dim player y 'Daniel Carvajal' en el csv
## Estos jugadores se insertan en la tabla como registros nuevos. En los jugadores equivalentes  que ya estan en dim_players  el campo id_transfermartk queda en null 


# ══════════════════════════════════════════════════
#  INSERTA LESIONES
# ══════════════════════════════════════════════════
def load_injuries(df_injuries,session):
    """ 
        Inyecta en la tabla fact_injuries  los datos de las lesiones de jugadores extraidos de Transfermarkt

        Requiere que previamente se haya insertado en dim_players el campo id_transfermarkt

    """

    for row  in df_injuries.itertuples():
        
        
        # obtiene registro donde el valor del campo id_transfermarkt coincida con el valor de la columna del DataFrame player_id( el id del jugados en transfermarkt)
        player= session.query(DimPlayer).filter(
            DimPlayer.id_transfermarkt == row.player_id
        ).first()
        
        if not player: 
            print(f'[NO ENCONTRADO] id_transfermarkt={row.player_id}')
            continue
        
        # hay que ver el tipo de dato  en las columnas Dataframe  y  el tipo de dato exigido por el Modelo y la tabla. 
        try:
            injury = FactInjuries(
                player_id=player.player_id,
                season=row.season,
                injury_type=row.injury_type,
                date_from=safe_date(row.date_from),
                date_until=safe_date(row.date_until),
                days_absent=int(row.days_absent) if pd.notna(row.days_absent) else None,
                matches_missed=int(row.matches_missed) if pd.notna(row.matches_missed) else None,
            )
            #crea un savepoint dentro de la transacción principal, de forma que si falla solo se deshace ese savepoint, no toda la sesión.
            with session.begin_nested():
                session.add(injury)
        except Exception as e:  
            print(f'[ERROR] player_id={row.player_id} | {e}')
            continue


    print('Lesiones cargadas') 


# ══════════════════════════════════════════════════
#  FUNCIÓN PRINCIPAL 
# ══════════════════════════════════════════════════

def load_transfermarkt(): 
    """ 
    Obtiene los DataFrames de los csv 
    Normaliza campos, inserta id_tranfermarkt en dim_player y lesiones en fact_injuries

    """
    
    # El scrapper extrae los fichares en ese ruta por lo que se buscan ahi 
    #lee el csv  de jugadores  y guarda en un DataFrame
    df_players = pd.read_csv(os.path.join(ROOT, 'data', 'raw', 'transfermarkt', 'transfermarket_players.csv'))

    # lee el csv de injuries y guarda en un DataFrame 
    df_injuries = pd.read_csv(os.path.join(ROOT,'data','raw','transfermarkt','transfermarket_injuries.csv'))

    with session_scope() as session:

        #1. Normaliza la columna positon en el Dataframe para que se ajuste al campo player_position en dim_players
        df_players= map_positions(df_players)

        #2. Incorpora birth_date y nationality al DataFrame 
        df_players = enrich_players_df(df_players, session)
        
        # 3. Actualiza en dim_players el campo id_transfermarkt
        update_transfermarkt_ids_in_dim_players(df_players, session)

        # 4. INSERT  en la tabla fact_injuries
        load_injuries(df_injuries,session)
    
        
     
if __name__ == "__main__":
   
   load_transfermarkt()

    

