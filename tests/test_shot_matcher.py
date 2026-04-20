import pytest
from utils.shot_matcher import es_mismo_tiro, prioridad_fuente

def test_detectar_tiro_duplicado():
    """Prueba si el matcher detecta que dos tiros casi iguales son el mismo"""
    # Un tiro que ya estaría en nuestra base de datos
    tiro_existente = {
        'shot_id': 100,
        'minute': 25,
        'x': 80.0,
        'y': 40.0,
        'player_id': 10
    }
    
    # Un tiro nuevo que viene de otra fuente (mismo minuto, casi misma posición)
    tiro_nuevo = {
        'minute': 25,
        'x': 80.5, # Solo 0.5 de diferencia
        'y': 40.2, # Solo 0.2 de diferencia
        'player_id': 10
    }
    
    resultado = es_mismo_tiro(tiro_nuevo, [tiro_existente])
    
    # Si el resultado es 100, es que ha detectado que es el mismo tiro
    assert resultado == 100
    print("\n Test de duplicado: ¡Pasado!")

def test_prioridad_fuentes():
    """Prueba que StatsBomb tenga más prioridad que SofaScore"""
    assert prioridad_fuente('statsbomb') > prioridad_fuente('sofascore')
    assert prioridad_fuente('understat') > prioridad_fuente('sofascore')
    print(" Test de prioridades: ¡Pasado!")

def test_tiro_diferente():
    """Prueba que si el minuto es muy distinto, no los confunda"""
    tiro_existente = {'shot_id': 100, 'minute': 10, 'x': 50, 'y': 50}
    tiro_nuevo = {'minute': 15, 'x': 50, 'y': 50} # 5 minutos de diferencia
    
    resultado = es_mismo_tiro(tiro_nuevo, [tiro_existente])
    
    assert resultado is None
    print(" Test de tiro diferente: ¡Pasado!")
