import pandas as pd
import os
import sys
from datetime import datetime
import numpy as np
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from dags.config import SILVER_PATH, GOLD_PATH

def extract_gold():
    """
    Transforma datos de la capa Silver a Gold:
    - Crea métricas de negocio
    - Agrega datos por idioma y año
    - Calcula estadísticas
    - Prepara datos para BI/Analytics
    """
    try:
        print("Extrayendo datos desde Silver hacia Gold...")
        
        # Validar que el archivo Silver existe
        if not os.path.exists(SILVER_PATH):
            raise FileNotFoundError(f"Archivo Silver no encontrado: {SILVER_PATH}")
        
        # Leer datos desde Silver
        print(f"Leyendo: {SILVER_PATH}")
        df_gold = pd.read_csv(SILVER_PATH, encoding="latin1")
        
        # Asegurar que release_date es datetime
        df_gold['release_date'] = pd.to_datetime(df_gold['release_date'], errors='coerce')
        
        print(f"\n=== DATOS ORIGINALES ===")
        print(f"Filas: {df_gold.shape[0]}, Columnas: {df_gold.shape[1]}")
        
        # TRANSFORMACIONES DE NEGOCIO
        
        # 1. Extraer año de lanzamiento
        df_gold['release_year'] = df_gold['release_date'].dt.year
        print("✓ Año de lanzamiento extraído")
        
        # 2. Extraer mes de lanzamiento
        df_gold['release_month'] = df_gold['release_date'].dt.month
        print("✓ Mes de lanzamiento extraído")
        
        # 3. Categorizar películas por calificación
        def categorizar_calificacion(rating):
            if pd.isna(rating):
                return 'Sin clasificar'
            elif rating >= 8:
                return 'Excelente'
            elif rating >= 7:
                return 'Muy buena'
            elif rating >= 6:
                return 'Buena'
            elif rating >= 5:
                return 'Regular'
            else:
                return 'Pobre'
        
        df_gold['calificacion_categoria'] = df_gold['vote_average'].apply(categorizar_calificacion)
        print("✓ Categoría de calificación creada")
        
        # 4. Categorizar por popularidad
        def categorizar_popularidad(popularity):
            if pd.isna(popularity):
                return 'Desconocida'
            elif popularity >= 50:
                return 'Muy popular'
            elif popularity >= 25:
                return 'Popular'
            elif popularity >= 10:
                return 'Moderadamente popular'
            else:
                return 'Poco popular'
        
        df_gold['popularidad_categoria'] = df_gold['popularity'].apply(categorizar_popularidad)
        print("✓ Categoría de popularidad creada")
        
        # 5. Crear indicador de película reciente
        año_actual = datetime.now().year
        df_gold['es_reciente'] = df_gold['release_year'] >= (año_actual - 5)
        print("✓ Indicador de película reciente creado")
        
        # 6. Crear agregaciones por idioma
        print("\n=== ESTADÍSTICAS POR IDIOMA ===")
        stats_idioma = df_gold.groupby('original_language').agg({
            'title': 'count',
            'vote_average': ['mean', 'median', 'std'],
            'popularity': ['mean', 'median'],
            'vote_count': 'sum'
        }).round(2)
        stats_idioma.columns = ['cantidad_peliculas', 'calificacion_promedio', 'calificacion_mediana', 
                                'calificacion_desv_est', 'popularidad_promedio', 'popularidad_mediana', 'votos_totales']
        print(stats_idioma)
        
        # 7. Crear agregaciones por año
        print("\n=== ESTADÍSTICAS POR AÑO ===")
        stats_year = df_gold.groupby('release_year').agg({
            'title': 'count',
            'vote_average': ['mean', 'max', 'min'],
            'popularity': 'mean'
        }).round(2)
        stats_year.columns = ['cantidad_peliculas', 'calificacion_promedio', 'calificacion_max', 
                             'calificacion_min', 'popularidad_promedio']
        print(stats_year.tail(10))
        
        # 8. Top películas por calificación
        print("\n=== TOP 10 PELÍCULAS POR CALIFICACIÓN ===")
        top_peliculas = df_gold.nlargest(10, 'vote_average')[['title', 'release_year', 'vote_average', 'popularity']]
        print(top_peliculas)
        
        # Preview de datos
        print(f"\n=== PREVIEW DEL DATAFRAME GOLD ===")
        print(f"Filas: {df_gold.shape[0]}, Columnas: {df_gold.shape[1]}")
        print(df_gold.head())
        print("\nColumnas finales:")
        print(df_gold.columns.tolist())
        print("================================\n")
        
        # Crear carpeta si no existe
        os.makedirs(os.path.dirname(GOLD_PATH), exist_ok=True)
        
        # Guardar en Gold
        df_gold.to_csv(GOLD_PATH, index=False, encoding='latin1')
        print(f"Guardado correctamente en: {GOLD_PATH}")
        print(f"Timestamp: {datetime.now().isoformat()}")
        
        return "OK"
    
    except Exception as e:
        print(f"Error en extract_gold: {str(e)}", file=sys.stderr)
        raise

if __name__ == "__main__":
    extract_gold()
