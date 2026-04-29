
"""
loaders/competition_loader.py
==============================
Carga dim_competition desde el diccionario COMPETITIONS de scripts/competitions.py.

Inserta o actualiza cada competición con:
    canonical_name, country, country_code,
    id_transfermarkt, id_sofascore, id_understat, id_statsbomb, id_whoscored
"""

import logging
from sqlalchemy import text
from loaders.common import engine 
from scripts.competitions import COMPETITIONS

log = logging.getLogger(__name__)

def load_competitions(conn) -> (int): 
        """
        Recorre el diccionario COMPETITIONS e inserta o actualiza cada competición
        en dim_competition.

        Devuelve:
                int: número de competiciones procesadas
        """
        log.info("[START] Cargando dim_competition...")
        count = 0

        for name, competition in COMPETITIONS.items(): 
                
                # SAVEPOINt . punto de control antes de INSERT al que volver si falla un INSERT concreto
                conn.execute(text(f"SAVEPOINT sp_{count}"))

                try:    
                        # extrar la clave sources ,  que es otro dict
                        sources = competition.get("sources",{})
                        # extrae el 'id' de cada fuente 
                        id_transfermarkt= sources.get("transfermarkt",{}).get("league_code")
                        id_sofascore= sources.get("sofascore",{}).get("tournament_id")
                        # understatt el campos parece ser varchar tb asi que hay que cambiarlo en la tabla 
                        id_understat= sources.get("understat",{}).get("league")
                        id_statsbomb= sources.get("statsbomb",{}).get("competition_id")
                        id_whoscored= sources.get("whoscored",{}).get("tournament_id")
                        
                        # EXCLUDED es una tabla virtual que PostgreSQL crea automáticamente con los valores que se intentaron insertar — los valores nuevos que llegaron en el INSERT.
                        # COALESCE  devuelve el primer arguento que no sea NULL 
                        # execute toma  la query y el diccionario como parametros 
                        conn.execute(text("""
                                INSERT INTO dim_competition
                                        (canonical_name, country, country_code,
                                        id_transfermarkt, id_sofascore, id_understat,
                                        id_statsbomb, id_whoscored)
                                VALUES
                                        (:name, :country, :country_code,
                                        :id_transfermarkt, :id_sofascore, :id_understat,
                                        :id_statsbomb, :id_whoscored)
                                ON CONFLICT (canonical_name) DO UPDATE SET
                                        country          = EXCLUDED.country,
                                        country_code     = EXCLUDED.country_code,
                                        id_transfermarkt = COALESCE(dim_competition.id_transfermarkt, EXCLUDED.id_transfermarkt),
                                        id_sofascore     = COALESCE(dim_competition.id_sofascore,     EXCLUDED.id_sofascore),
                                        id_understat     = COALESCE(dim_competition.id_understat,     EXCLUDED.id_understat),
                                        id_statsbomb     = COALESCE(dim_competition.id_statsbomb,     EXCLUDED.id_statsbomb),
                                        id_whoscored     = COALESCE(dim_competition.id_whoscored,     EXCLUDED.id_whoscored)

                        """) ,{
                               # El paramentro diccionario. Las claves tienen que coincidir con los placeholders usados en la consulta
                               "name":                name,
                                "country":          competition.get("country"),
                                "country_code":     competition.get("country_code"),
                                "id_transfermarkt": id_transfermarkt,
                                "id_sofascore":     id_sofascore,
                                "id_understat":     id_understat,
                                "id_statsbomb":     str(id_statsbomb) if id_statsbomb else None,
                                "id_whoscored":     id_whoscored,
                        })
                        #RELEASE SAVEPOINT  Si el INSERT  fue exitoso, elimina el punto de control. Ya no se necesita 
                        conn.execute(text(f"RELEASE SAVEPOINT sp_{count}"))

                        count += 1
                
                except Exception as e:
                        # Si un INSERT falla, se deshace el INSERT que falla, sin afectar  al resto de la transaccion
                        # Sin SAVEPOINTs, la transacción entera  queda en estado de error y los INSERT que no dieron fallo, no se guardan. 
                        conn.execute(text(f"ROLLBACK TO SAVEPOINT sp_{count}"))
                        log.error("Error insertando competición '%s': %s", name, e)
                        continue

        log.info("[OK] dim_competition completado — %d competiciones procesadas", count)
        return count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s - %(message)s")
    with engine.begin() as conn:
        load_competitions(conn)
                        