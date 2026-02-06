import pandas as pd
import pyodbc
from sqlalchemy import create_engine
import urllib
import os
from pathlib import Path
import logging
from datetime import datetime
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# =============================================================================
# CONFIGURACIÓN Y LOGGING
# =============================================================================

# Configurar logging
log_dir = Path('logs')
log_dir.mkdir(exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_file = log_dir / f"etl_transmision_{timestamp}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURACIÓN DE CONEXIÓN A SQL SERVER (desde variables de entorno)
# =============================================================================
server = os.getenv('SQL_SERVER')
database = os.getenv('SQL_DATABASE')
username = os.getenv('SQL_USERNAME')
password = os.getenv('SQL_PASSWORD')
driver = os.getenv('SQL_DRIVER', 'ODBC Driver 17 for SQL Server')
use_windows_auth = os.getenv('SQL_USE_WINDOWS_AUTH', 'false').lower() == 'true'

# Validar que las variables esenciales estén configuradas
if not server or not database:
    raise ValueError(
        "Error: Faltan variables de entorno para la conexión SQL.\n"
        "Variables requeridas:\n"
        "  - SQL_SERVER (ej: localhost\\SQLEXPRESS)\n"
        "  - SQL_DATABASE\n"
    )

# Construir connection string según el tipo de autenticación
if use_windows_auth:
    connection_string = f'DRIVER={{{driver}}};SERVER={server};DATABASE={database};Trusted_Connection=yes'
    logger.info("✓ Usando autenticación de Windows (Trusted Connection)")
else:
    if not username or not password:
        raise ValueError(
            "Error: Para autenticación SQL Server se requieren SQL_USERNAME y SQL_PASSWORD.\n"
            "O configura SQL_USE_WINDOWS_AUTH=true para usar autenticación de Windows."
        )
    connection_string = f'DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    logger.info("✓ Usando autenticación SQL Server")

params = urllib.parse.quote_plus(connection_string)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

logger.info("✓ Configuración SQL cargada desde variables de entorno")
logger.info(f"  - Servidor: {server}")
logger.info(f"  - Base de datos: {database}")
if not use_windows_auth:
    logger.info(f"  - Usuario: {username}")

# =============================================================================
# CONFIGURACIÓN DEL ARCHIVO EXCEL
# =============================================================================
carpeta_excel = os.getenv('EXCEL_FOLDER_TRANSMISION', os.path.join(os.getcwd(), 'datos_transmision'))
nombre_tabla_sql = os.getenv('SQL_TABLE_NAME', 'Calidad_Transmision')

if not os.path.exists(carpeta_excel):
    logger.warning(f"⚠️  La carpeta {carpeta_excel} no existe. Configura EXCEL_FOLDER_TRANSMISION en .env")

# =============================================================================
# MAPEO DE COLUMNAS: Excel → SQL Server
# =============================================================================

COLUMN_MAPPING = {
    'FECHA_HORA_APERTURA': 'FECHA_HORA_APERTURA',
    'FECHA_HORA_CIERRE': 'FECHA_HORA_CIERRE',
    'DURACIÓN_INDISPONIBILIDAD_MINUTOS': 'DURACION_INDISPONIBILIDAD_MINUTOS',
    'CARGA_MEGAS': 'CARGA_MEGAS',
    'CODIGO_ELEMENTO_AFECTADO': 'CODIGO_ELEMENTO_AFECTADO',
    'TIPO_EQUIPO': 'TIPO_EQUIPO',
    'CIRCUITOS_AFECTADOS': 'CIRCUITOS_AFECTADOS',
    'SUBESTACION': 'SUBESTACION',
    'REGION': 'REGION',
    'CODIGO_INTERRUPTOR': 'CODIGO_INTERRUPTOR',
    'NIVEL_DE_TENSION': 'NIVEL_DE_TENSION',
    'PROTECCION _OPERADA': 'PROTECCION_OPERADA',  # Corrige espacio extra
    'ORIGEN_INDISPONIBILIDAD': 'ORIGEN_INDISPONIBILIDAD',
    'CAUSA_EVENTO': 'CAUSA_EVENTO',
    'EXCEPCIONES': 'EXCEPCIONES',
    'TIPO_INDISPONIBILIDAD': 'TIPO_INDISPONIBILIDAD',
    'TIPO_MANTENIMIENTO': 'TIPO_MANTENIMIENTO',
    'DESCRIPCION_EVENTO': 'DESCRIPCION_EVENTO',
}

# Columnas de fecha que necesitan conversión
DATE_COLUMNS = ['FECHA_HORA_APERTURA', 'FECHA_HORA_CIERRE']

# Columnas numéricas que necesitan conversión
NUMERIC_COLUMNS = ['DURACION_INDISPONIBILIDAD_MINUTOS', 'CARGA_MEGAS']

# =============================================================================
# FUNCIONES DE PROCESAMIENTO
# =============================================================================

def obtener_archivos_excel(carpeta):
    """Obtiene todos los archivos Excel (.xlsx, .xls) de una carpeta"""
    ruta = Path(carpeta)
    archivos_excel = []
    
    for extension in ['*.xlsx', '*.xls']:
        archivos_excel.extend(ruta.glob(extension))
    
    return sorted(archivos_excel)

def limpiar_y_preparar_datos(df):
    """Limpia y prepara el DataFrame antes de cargarlo a SQL"""
    logger.info(f"\n{'='*60}")
    logger.info("LIMPIEZA Y PREPARACIÓN DE DATOS")
    logger.info(f"{'='*60}")
    
    filas_iniciales = len(df)
    logger.info(f"Filas iniciales: {filas_iniciales}")
    logger.info(f"Columnas iniciales: {list(df.columns)}")
    
    # 1. Mapear columnas
    logger.info("\n1. Mapeando columnas...")
    columnas_a_renombrar = {}
    
    for col_excel in df.columns:
        if col_excel in COLUMN_MAPPING:
            columnas_a_renombrar[col_excel] = COLUMN_MAPPING[col_excel]
            logger.info(f"   ✓ '{col_excel}' → '{COLUMN_MAPPING[col_excel]}'")
    
    if columnas_a_renombrar:
        df = df.rename(columns=columnas_a_renombrar)
    
    # Mantener solo columnas mapeadas
    columnas_validas = list(COLUMN_MAPPING.values())
    columnas_a_mantener = [col for col in df.columns if col in columnas_validas]
    df = df[columnas_a_mantener]
    
    logger.info(f"\n   Columnas finales: {list(df.columns)}")
    
    # 2. Eliminar filas completamente vacías
    logger.info("\n2. Eliminando filas vacías...")
    df = df.dropna(how='all')
    logger.info(f"   ✓ Filas después de eliminar vacías: {len(df)}")
    
    # 3. Eliminar filas donde FECHA_HORA_APERTURA es NaN (campo crítico)
    logger.info("\n3. Validando fecha de apertura...")
    if 'FECHA_HORA_APERTURA' in df.columns:
        df = df.dropna(subset=['FECHA_HORA_APERTURA'])
        logger.info(f"   ✓ Filas con fecha válida: {len(df)}")
    
    # 4. Convertir columnas de fecha
    logger.info("\n4. Convirtiendo columnas de fecha...")
    for col in DATE_COLUMNS:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                valores_nulos = df[col].isna().sum()
                logger.info(f"   ✓ '{col}' convertida a datetime ({valores_nulos} valores nulos)")
            except Exception as e:
                logger.warning(f"   ⚠️  No se pudo convertir '{col}': {e}")
    
    # 5. Convertir columnas numéricas
    logger.info("\n5. Convirtiendo columnas numéricas...")
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                valores_nulos = df[col].isna().sum()
                logger.info(f"   ✓ '{col}' convertida a numérico ({valores_nulos} valores nulos)")
            except Exception as e:
                logger.warning(f"   ⚠️  No se pudo convertir '{col}': {e}")
    
    # 6. Limpiar espacios en blanco en strings
    logger.info("\n6. Limpiando espacios en strings...")
    for col in df.select_dtypes(include=['object']):
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    
    # 7. Reemplazar valores vacíos con None
    df = df.replace({pd.NA: None, pd.NaT: None, '': None})
    
    # 8. Verificar límites de columnas
    logger.info("\n7. Verificando límites de columnas...")
    for col in df.select_dtypes(include=['object']):
        max_len = df[col].astype(str).str.len().max()
        if max_len > 255:
            logger.warning(f"   ⚠️  '{col}': valores hasta {max_len} caracteres (límite SQL típico: 255)")
        elif max_len > 4000:
            logger.warning(f"   ⚠️  '{col}': valores hasta {max_len} caracteres (considerar NVARCHAR(MAX))")
    
    logger.info(f"\n{'='*60}")
    logger.info(f"✓ Limpieza completada:")
    logger.info(f"  - Filas finales: {len(df)} (de {filas_iniciales} iniciales)")
    logger.info(f"  - Columnas finales: {len(df.columns)}")
    logger.info(f"{'='*60}")
    
    return df

def procesar_archivo(archivo_excel, engine, nombre_tabla, if_exists='append'):
    """
    Procesa un archivo Excel y carga la hoja FORMATO a SQL Server
    
    Args:
        archivo_excel: Ruta del archivo Excel
        engine: Motor de conexión SQLAlchemy
        nombre_tabla: Nombre de la tabla SQL destino
        if_exists: 'append' o 'replace'
    
    Returns:
        Dict con resultado de la operación
    """
    nombre_archivo = os.path.basename(archivo_excel)
    
    logger.info(f"\n{'#'*60}")
    logger.info(f"PROCESANDO ARCHIVO: {nombre_archivo}")
    logger.info(f"{'#'*60}")
    
    try:
        # Verificar que la hoja FORMATO existe
        xls = pd.ExcelFile(archivo_excel)
        if 'FORMATO' not in xls.sheet_names:
            logger.error(f"✗ Hoja 'FORMATO' no encontrada en el archivo")
            logger.info(f"  Hojas disponibles: {xls.sheet_names}")
            return {
                'archivo': nombre_archivo,
                'estado': 'error',
                'mensaje': "Hoja 'FORMATO' no encontrada"
            }
        
        # Leer la hoja FORMATO
        logger.info(f"\nLeyendo hoja 'FORMATO'...")
        df = pd.read_excel(archivo_excel, sheet_name='FORMATO')
        logger.info(f"✓ Datos leídos: {len(df)} filas, {len(df.columns)} columnas")
        
        # Limpiar y preparar datos
        df = limpiar_y_preparar_datos(df)
        
        if len(df) == 0:
            logger.warning("⚠️  No hay datos válidos para insertar después de la limpieza")
            return {
                'archivo': nombre_archivo,
                'estado': 'sin_datos',
                'filas': 0
            }
        
        # Mostrar muestra de datos
        logger.info("\nMuestra de datos (primeras 3 filas):")
        for i, row in df.head(3).iterrows():
            logger.info(f"  Fila {i}: {row.to_dict()}")
        
        # Cargar a SQL Server
        logger.info(f"\n{'='*60}")
        logger.info(f"CARGANDO DATOS A SQL SERVER")
        logger.info(f"{'='*60}")
        logger.info(f"Tabla destino: {nombre_tabla}")
        logger.info(f"Modo: {if_exists}")
        logger.info(f"Filas a insertar: {len(df)}")
        
        df.to_sql(
            name=nombre_tabla,
            con=engine,
            if_exists=if_exists,
            index=False,
            chunksize=500
        )
        
        logger.info(f"\n✓ Datos cargados exitosamente a la tabla '{nombre_tabla}'")
        
        return {
            'archivo': nombre_archivo,
            'estado': 'éxito',
            'filas': len(df)
        }
        
    except Exception as e:
        logger.error(f"\n✗ Error procesando archivo: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'archivo': nombre_archivo,
            'estado': 'error',
            'mensaje': str(e)
        }

# =============================================================================
# EJECUTAR EL PROCESO
# =============================================================================

if __name__ == "__main__":
    logger.info("="*60)
    logger.info("ETL CALIDAD DE TRANSMISIÓN - HOJA FORMATO")
    logger.info("="*60)
    
    try:
        # Obtener archivos Excel
        archivos = obtener_archivos_excel(carpeta_excel)
        
        if not archivos:
            logger.error(f"\n✗ No se encontraron archivos Excel en: {carpeta_excel}")
            exit()
        
        logger.info(f"\n✓ Se encontraron {len(archivos)} archivo(s) Excel:")
        for idx, archivo in enumerate(archivos, 1):
            logger.info(f"  {idx}. {archivo.name}")
        
        # Procesar cada archivo
        resultados = []
        
        for archivo in archivos:
            resultado = procesar_archivo(
                archivo_excel=str(archivo),
                engine=engine,
                nombre_tabla=nombre_tabla_sql,
                if_exists='append'  # Cambiar a 'replace' si necesitas reemplazar
            )
            resultados.append(resultado)
        
        # Resumen final
        logger.info("\n" + "="*60)
        logger.info("RESUMEN FINAL")
        logger.info("="*60)
        
        exitosos = 0
        errores = 0
        sin_datos = 0
        total_filas = 0
        
        for resultado in resultados:
            if resultado['estado'] == 'éxito':
                logger.info(f"✓ {resultado['archivo']}: {resultado['filas']} filas cargadas")
                exitosos += 1
                total_filas += resultado['filas']
            elif resultado['estado'] == 'sin_datos':
                logger.info(f"⊘ {resultado['archivo']}: Sin datos válidos")
                sin_datos += 1
            else:
                logger.error(f"✗ {resultado['archivo']}: ERROR - {resultado['mensaje']}")
                errores += 1
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Archivos procesados exitosamente: {exitosos}")
        logger.info(f"Archivos sin datos: {sin_datos}")
        logger.info(f"Archivos con errores: {errores}")
        logger.info(f"Total de filas cargadas: {total_filas}")
        logger.info(f"{'='*60}")
        
        logger.info(f"\n✓ Proceso completado")
        logger.info(f"📄 Log guardado en: {log_file}")
        
    except Exception as e:
        logger.error(f"\n✗ Error general: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    
    finally:
        engine.dispose()
        logger.info("\n🔌 Conexión cerrada")