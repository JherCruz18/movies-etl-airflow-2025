import pandas as pd
import os
import sys
from datetime import datetime
import numpy as np
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from dags.config import BRONZE_PATH, SILVER_PATH

def extract_silver():
    """
    Transforma datos de la capa Bronze a Silver:
    - Limpia datos faltantes
    - Normaliza tipos de datos
    - Elimina duplicados
    - Valida calidad de datos
    """
    try:
        print("Extrayendo datos desde Bronze hacia Silver...")
        
        # Validar que el archivo Bronze existe
        if not os.path.exists(BRONZE_PATH):
            raise FileNotFoundError(f"Archivo Bronze no encontrado: {BRONZE_PATH}")
        
        # Leer datos desde Bronze
        print(f"Leyendo: {BRONZE_PATH}")
        df_silver = pd.read_csv(BRONZE_PATH, encoding="latin1")
        
        print(f"\n=== ANTES DE TRANSFORMACIÓN ===")
        print(f"Filas: {df_silver.shape[0]}, Columnas: {df_silver.shape[1]}")
        print(f"Valores nulos:\n{df_silver.isnull().sum()}")
        
        # TRANSFORMACIONES
        
        # 1. Eliminar columna índice innecesaria
        if 'Unnamed: 0' in df_silver.columns:
            df_silver = df_silver.drop(columns=['Unnamed: 0'])
            print("\n✓ Columna índice eliminada")
        
        # 2. Limpiar espacios en blanco en columnas de texto
        string_columns = df_silver.select_dtypes(include=['object']).columns
        for col in string_columns:
            df_silver[col] = df_silver[col].str.strip()
        print("✓ Espacios en blanco eliminados")
        
        # 3. Convertir columnas de fechas
        df_silver['release_date'] = pd.to_datetime(df_silver['release_date'], errors='coerce')
        print("✓ Fechas convertidas al formato correcto")
        
        # 4. Eliminar duplicados
        duplicados_antes = len(df_silver)
        df_silver = df_silver.drop_duplicates()
        duplicados_eliminados = duplicados_antes - len(df_silver)
        if duplicados_eliminados > 0:
            print(f"✓ {duplicados_eliminados} duplicados eliminados")
        
        # 5. Rellenar valores nulos en columnas numéricas
        numeric_columns = df_silver.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            if df_silver[col].isnull().sum() > 0:
                df_silver[col] = df_silver[col].fillna(df_silver[col].mean())
        print("✓ Valores nulos en columnas numéricas rellenados con la media")
        
        # 6. Rellenar valores nulos en columnas de texto
        text_columns = df_silver.select_dtypes(include=['object']).columns
        for col in text_columns:
            if df_silver[col].isnull().sum() > 0:
                df_silver[col] = df_silver[col].fillna('Desconocido')
        print("✓ Valores nulos en columnas de texto rellenados")
        
        # 7. Validar rango de valores (ej: vote_average debe estar entre 0 y 10)
        if 'vote_average' in df_silver.columns:
            df_silver = df_silver[(df_silver['vote_average'] >= 0) & (df_silver['vote_average'] <= 10)]
            print("✓ Valores de vote_average validados (0-10)")
        
        print(f"\n=== DESPUÉS DE TRANSFORMACIÓN ===")
        print(f"Filas: {df_silver.shape[0]}, Columnas: {df_silver.shape[1]}")
        print(f"Valores nulos:\n{df_silver.isnull().sum()}")
        
        # Preview de datos
        print(f"\n=== PREVIEW DEL DATAFRAME SILVER ===")
        print(df_silver.head())
        print("\nTipos de datos:")
        print(df_silver.dtypes)
        print("================================\n")
        
        # Crear carpeta si no existe
        os.makedirs(os.path.dirname(SILVER_PATH), exist_ok=True)
        
        # Guardar en Silver
        df_silver.to_csv(SILVER_PATH, index=False, encoding='latin1')
        print(f"Guardado correctamente en: {SILVER_PATH}")
        print(f"Timestamp: {datetime.now().isoformat()}")
        
        return "OK"
    
    except Exception as e:
        print(f"Error en extract_silver: {str(e)}", file=sys.stderr)
        raise

if __name__ == "__main__":
    extract_silver()
