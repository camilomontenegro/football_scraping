import sys
from pathlib import Path
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).parent.parent))

from loaders.common import engine
from utils.mdm_engine import _get_player_cache, _similarity_score, normalize
from utils.mdm_config import SOURCE_ID_FIELDS

def reprocess_reviews():
    print("\n========================================================")
    print(" REPROCESANDO JUGADORES PENDIENTES CON NUEVO ALGORITMO  ")
    print("========================================================\n")
    
    with engine.begin() as conn:
        cache = _get_player_cache(conn)
        
        # Obtener jugadores que siguen sin resolver
        rows = conn.execute(text("""
            SELECT id, source_name, source_system, source_id
            FROM player_review
            WHERE resolved = FALSE
        """)).fetchall()
        
        if not rows:
            print("¡No hay jugadores pendientes de revisión!")
            return
            
        print(f"Evaluando {len(rows)} jugadores...\n")
        
        auto_linked = 0
        auto_created = 0
        left_pending = 0
        
        for rev_id, source_name, source_system, source_id in rows:
            id_col = SOURCE_ID_FIELDS.get(source_system, {}).get("player")
            if not id_col:
                continue
                
            norm = normalize(source_name)
            if not norm:
                continue
                
            best_score = 0
            best_id = None
            
            for p in cache:
                score = _similarity_score(norm, p["norm"])
                if score > best_score:
                    best_score = score
                    best_id = p["id"]
            
            print(f"------------------------------------------------")
            print(f"Fuente: {source_name} ({source_system})")
            if best_id:
                suggested_name = next((p["name"] for p in cache if p["id"] == best_id), "Desconocido")
                print(f"Mejor en BD: {suggested_name} (Similitud: {best_score}%)")
            else:
                print("Mejor en BD: NINGUNA")

            is_single_word = len(norm.split()) == 1
            auto_accept = False
            auto_new = False
            
            if best_id:
                if best_score >= 95:
                    auto_accept = True
                elif best_score >= 90 and not is_single_word:
                    auto_accept = True
                    
            if not auto_accept and best_score < 50:
                auto_new = True

            if auto_accept:
                resp = 's'
                print("--> [AUTO-ACEPTADO] (Criterio estricto de confianza)")
            elif auto_new:
                resp = 'n'
                print("--> [AUTO-NUEVO] (Similitud muy baja)")
            else:
                if best_score >= 85 and best_id:
                    default = 's'
                    prompt = "¿Qué hacemos? [S]on el mismo / (N)uevo / (I)gnorar (Enter para confirmar S): "
                else:
                    default = 'i'
                    prompt = "¿Qué hacemos? (S)on el mismo / (N)uevo / [I]gnorar (Enter para dejar en I): "

                while True:
                    resp = input(prompt).strip().lower()
                    if resp == '':
                        resp = default
                    if resp in ['s', 'n', 'i']:
                        break
                    print("Opción inválida.")

            if resp == 's':
                if not best_id:
                    print("¡No hay sugerencia para enlazar! Lo crearemos como nuevo.")
                    resp = 'n'
                else:
                    conn.execute(text(f"""
                        UPDATE dim_player SET {id_col} = :sid 
                        WHERE canonical_id = :cid AND {id_col} IS NULL
                    """), {"sid": source_id, "cid": best_id})
                    
                    conn.execute(text("""
                        UPDATE player_review 
                        SET resolved = TRUE, canonical_id_assigned = :cid, similarity_score = :score, reviewed_at = NOW()
                        WHERE id = :rid
                    """), {"cid": best_id, "score": best_score, "rid": rev_id})
                    
                    auto_linked += 1
                    print("--> [ENLAZADO Y ACTUALIZADO]")
                
            if resp == 'n':
                new_id = conn.execute(text(f"""
                    INSERT INTO dim_player (canonical_name, {id_col})
                    VALUES (:name, :sid)
                    RETURNING canonical_id
                """), {"name": source_name, "sid": source_id}).scalar()
                
                conn.execute(text("""
                    UPDATE player_review 
                    SET resolved = TRUE, canonical_id_assigned = :cid, similarity_score = :score, reviewed_at = NOW()
                    WHERE id = :rid
                """), {"cid": new_id, "score": best_score, "rid": rev_id})
                
                cache.append({"id": new_id, "name": source_name, "norm": norm})
                auto_created += 1
                print("--> [CREADO COMO NUEVO JUGADOR]")
                
            if resp == 'i':
                conn.execute(text("""
                    UPDATE player_review
                    SET suggested_canonical_id = :cid, similarity_score = :score
                    WHERE id = :rid
                """), {"cid": best_id, "score": best_score, "rid": rev_id})
                left_pending += 1
                print("--> [IGNORADO POR AHORA]")

        print("\n========================================================")
        print(" RESUMEN DEL PROCESO ")
        print("========================================================")
        print(f" [OK] Enlazados automáticamente (>= 85%): {auto_linked}")
        print(f" [OK] Creados como nuevos (< 50%):        {auto_created}")
        print(f" [!] Dejados pendientes por ti:           {left_pending}")
        print("========================================================\n")

if __name__ == "__main__":
    try:
        reprocess_reviews()
    except KeyboardInterrupt:
        print("\n\nProceso cancelado por el usuario. Los cambios hasta ahora se han guardado.")
