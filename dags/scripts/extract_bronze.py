import pandas as pd
import os
import sys
from datetime import datetime
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from dags.config import ORIGIN_PATH, BRONZE_PATH

def extract_bronze():
    """Extrae datos del archivo CSV y los guarda en formato Parquet en la capa Bronze"""
    try:
        print("Extrayendo datos hacia Bronze...")
        
        # Validar que el archivo origen existe
        if not os.path.exists(ORIGIN_PATH):
            raise FileNotFoundError(f"Archivo origen no encontrado: {ORIGIN_PATH}")
        
        # Leer CSV
        print(f"Leyendo: {ORIGIN_PATH}")
        df_bronze = pd.read_csv(ORIGIN_PATH, encoding="latin1", sep=",", engine='python')
        
        # Validar que se cargaron datos
        if df_bronze.empty:
            raise ValueError("El archivo CSV está vacío")
        
        # Normalizar y limpiar datos
        df_bronze = df_bronze.astype(str).apply(lambda col: col.str.strip())
        
        # Preview
        print("\n=== PREVIEW DEL DATAFRAME ===")
        print(f"Filas: {df_bronze.shape[0]}, Columnas: {df_bronze.shape[1]}")
        print(df_bronze.head())
        print("================================\n")
        
        # Crear carpeta si no existe
        os.makedirs(os.path.dirname(BRONZE_PATH), exist_ok=True)
        
        # Guardar como CSV en Bronze
        df_bronze.to_csv(BRONZE_PATH, index=False, encoding='latin1')
        print(f"Guardado correctamente en: {BRONZE_PATH}")
        print(f"Timestamp: {datetime.now().isoformat()}")
        
        return "OK"
    
    except Exception as e:
        print(f"Error en extract_bronze: {str(e)}", file=sys.stderr)
        raise

if __name__ == "__main__":
    extract_bronze()