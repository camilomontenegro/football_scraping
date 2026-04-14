
import pandas as pd

def limpiar_y_transformar_tiros(df):
    
    """
    Recibe el DataFrame crudo y aplica limpieza y transformación.

    """
    # 1. Estandarizar nombres de columnas a minúsculas
    df.columns = [str(col).lower() for col in df.columns]
    
    # 2. Conversión de tipos (Casting)
    columnas_num = ['x', 'y', 'xg']
    for col in columnas_num:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # 3. Transformación a Metros Reales
    df['x_metres'] = df['x'] * 105
    df['y_metres'] = df['y'] * 68
    
    return df