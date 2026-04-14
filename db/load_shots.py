import pandas as pd
from db.models import get_engine, FactShots
from sqlalchemy.orm import sessionmaker

def probar_conexion():
    engine = get_engine()
    print("¡Motor de base de datos conectado correctamente!")
    
    # Crear una sesión para interactuar
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Comprobar si la tabla tiene datos (solo para probar)
    conteo = session.query(FactShots).count()
    print(f"Actualmente hay {conteo} tiros en la base de datos.")
    
    session.close()

if __name__ == "__main__":
    probar_conexion()

