import pandas as pd
import pyodbc
from sqlalchemy import create_engine, text
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
# CONFIGURACIÓN DE CONEXIÓN A SQL SERVER
# =============================================================================
server = os.getenv('SQL_SERVER')
database = os.getenv('SQL_DATABASE')
username = os.getenv('SQL_USERNAME')
password = os.getenv('SQL_PASSWORD')
driver = os.getenv('SQL_DRIVER', 'ODBC Driver 17 for SQL Server')
use_windows_auth = os.getenv('SQL_USE_WINDOWS_AUTH', 'false').lower() == 'true'

# Validar configuración
if not server or not database:
    raise ValueError("Error: Faltan variables SQL_SERVER y/o SQL_DATABASE en .env")

# Construir connection string
if use_windows_auth:
    connection_string = f'DRIVER={{{driver}}};SERVER={server};DATABASE={database};Trusted_Connection=yes'
    logger.info("✓ Usando autenticación de Windows")
else:
    if not username or not password:
        raise ValueError("Error: Se requieren SQL_USERNAME y SQL_PASSWORD")
    connection_string = f'DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    logger.info("✓ Usando autenticación SQL Server")

params = urllib.parse.quote_plus(connection_string)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

logger.info(f"✓ Servidor: {server}")
logger.info(f"✓ Base de datos: {database}")

# =============================================================================
# CONFIGURACIÓN DEL ETL
# =============================================================================
carpeta_excel = os.getenv('EXCEL_FOLDER_TRANSMISION', os.path.join(os.getcwd(), 'datos_transmision'))
nombre_tabla_sql = os.getenv('SQL_TABLE_NAME', 'Calidad_Transmision')
modo_interactivo = os.getenv('ETL_MODO_INTERACTIVO', 'true').lower() == 'true'

# =============================================================================
# MAPEO DE COLUMNAS
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
    'PROTECCION _OPERADA': 'PROTECCION_OPERADA',
    'ORIGEN_INDISPONIBILIDAD': 'ORIGEN_INDISPONIBILIDAD',
    'CAUSA_EVENTO': 'CAUSA_EVENTO',
    'EXCEPCIONES': 'EXCEPCIONES',
    'TIPO_INDISPONIBILIDAD': 'TIPO_INDISPONIBILIDAD',
    'TIPO_MANTENIMIENTO': 'TIPO_MANTENIMIENTO',
    'DESCRIPCION_EVENTO': 'DESCRIPCION_EVENTO',
}

DATE_COLUMNS = ['FECHA_HORA_APERTURA', 'FECHA_HORA_CIERRE']
NUMERIC_COLUMNS = ['DURACION_INDISPONIBILIDAD_MINUTOS', 'CARGA_MEGAS']

# =============================================================================
# FUNCIONES DE VERIFICACIÓN DE ARCHIVOS
# =============================================================================

def verificar_archivo_ya_cargado(engine, nombre_archivo):
    """
    Verifica si un archivo ya fue cargado previamente
    
    Returns:
        dict con información del archivo o None si no existe
    """
    try:
        query = text("""
            EXEC sp_Verificar_Archivo_Cargado @NombreArchivo = :nombre_archivo
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {"nombre_archivo": nombre_archivo})
            row = result.fetchone()
            
            if row and row[0] > 0:  # Total_Registros > 0
                return {
                    'existe': True,
                    'total_registros': row[0],
                    'primera_carga': row[1],
                    'ultima_carga': row[2],
                    'ultima_actualizacion': row[3],
                    'evento_mas_antiguo': row[4],
                    'evento_mas_reciente': row[5]
                }
            else:
                return {'existe': False}
                
    except Exception as e:
        logger.warning(f"No se pudo verificar archivo (probablemente SP no existe): {e}")
        return {'existe': False}

def eliminar_datos_archivo(engine, nombre_archivo):
    """
    Elimina todos los registros de un archivo específico
    
    Returns:
        Número de registros eliminados
    """
    try:
        query = text("""
            DECLARE @RegistrosEliminados INT;
            EXEC sp_Eliminar_Datos_Archivo 
                @NombreArchivo = :nombre_archivo,
                @RegistrosEliminados = @RegistrosEliminados OUTPUT;
            SELECT @RegistrosEliminados;
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {"nombre_archivo": nombre_archivo})
            registros_eliminados = result.scalar()
            conn.commit()
            return registros_eliminados
            
    except Exception as e:
        logger.error(f"Error al eliminar datos del archivo: {e}")
        return 0

def solicitar_accion_usuario(info_archivo):
    """
    Pregunta al usuario qué hacer con un archivo duplicado
    
    Returns:
        'skip', 'replace', o 'append'
    """
    print("\n" + "="*60)
    print("⚠️  ARCHIVO YA CARGADO PREVIAMENTE")
    print("="*60)
    print(f"📊 Total de registros existentes: {info_archivo['total_registros']}")
    print(f"📅 Primera carga: {info_archivo['primera_carga']}")
    print(f"📅 Última carga: {info_archivo['ultima_carga']}")
    
    if info_archivo['ultima_actualizacion']:
        print(f"🔄 Última actualización: {info_archivo['ultima_actualizacion']}")
    
    print(f"\n📆 Rango de eventos: {info_archivo['evento_mas_antiguo']} a {info_archivo['evento_mas_reciente']}")
    
    print("\n" + "="*60)
    print("¿Qué deseas hacer con este archivo?")
    print("="*60)
    print("  1️⃣  SALTAR")
    print("      └─ No cargar este archivo")
    print("      └─ Mantener los datos existentes en la base de datos")
    print("")
    print("  2️⃣  REEMPLAZAR")
    print("      └─ Eliminar los {0} registros antiguos".format(info_archivo['total_registros']))
    print("      └─ Cargar los datos nuevos del archivo")
    print("      └─ Útil cuando el archivo tiene correcciones o actualizaciones")
    print("")
    print("  3️⃣  AGREGAR")
    print("      └─ Mantener los datos antiguos en la base de datos")
    print("      └─ Agregar los datos nuevos (⚠️  creará duplicados)")
    print("      └─ Solo usar si necesitas mantener ambas versiones")
    
    while True:
        print("\n" + "-"*60)
        opcion = input(" Selecciona una opción (1, 2 o 3): ").strip()
        
        if opcion == '1':
            print("✓ Has seleccionado: SALTAR archivo")
            confirmacion = input("  ¿Confirmas? (S/N): ").strip().upper()
            if confirmacion == 'S':
                logger.info("Usuario eligió: SALTAR archivo")
                return 'skip'
        elif opcion == '2':
            print("⚠️  Has seleccionado: REEMPLAZAR datos")
            print(f"  Se eliminarán {info_archivo['total_registros']} registros existentes")
            confirmacion = input("  ¿Confirmas? (S/N): ").strip().upper()
            if confirmacion == 'S':
                logger.info("Usuario eligió: REEMPLAZAR datos")
                return 'replace'
        elif opcion == '3':
            print("⚠️  Has seleccionado: AGREGAR datos (creará duplicados)")
            confirmacion = input("  ¿Confirmas? (S/N): ").strip().upper()
            if confirmacion == 'S':
                logger.info("Usuario eligió: AGREGAR datos (duplicados)")
                return 'append'
        else:
            print("❌ Opción inválida. Por favor selecciona 1, 2 o 3")

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

def limpiar_y_preparar_datos(df, nombre_archivo):
    """Limpia y prepara el DataFrame, agregando columna de archivo origen"""
    logger.info(f"\n{'='*60}")
    logger.info("LIMPIEZA Y PREPARACIÓN DE DATOS")
    logger.info(f"{'='*60}")
    
    filas_iniciales = len(df)
    
    # 1. Mapear columnas
    logger.info("\n1. Mapeando columnas...")
    columnas_a_renombrar = {}
    
    for col_excel in df.columns:
        if col_excel in COLUMN_MAPPING:
            columnas_a_renombrar[col_excel] = COLUMN_MAPPING[col_excel]
    
    if columnas_a_renombrar:
        df = df.rename(columns=columnas_a_renombrar)
    
    columnas_validas = list(COLUMN_MAPPING.values())
    columnas_a_mantener = [col for col in df.columns if col in columnas_validas]
    df = df[columnas_a_mantener]
    
    # 2. Eliminar filas vacías
    logger.info("\n2. Eliminando filas vacías...")
    df = df.dropna(how='all')
    
    # 3. Filtrar filas sin fecha de apertura
    logger.info("\n3. Validando fecha de apertura...")
    if 'FECHA_HORA_APERTURA' in df.columns:
        df = df.dropna(subset=['FECHA_HORA_APERTURA'])
        logger.info(f"   ✓ Filas con fecha válida: {len(df)}")
    
    # 4. Convertir fechas
    logger.info("\n4. Convirtiendo columnas de fecha...")
    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # 5. Convertir numéricos
    logger.info("\n5. Convirtiendo columnas numéricas...")
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 6. Limpiar strings
    for col in df.select_dtypes(include=['object']):
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    
    # 7. Reemplazar valores vacíos
    df = df.replace({pd.NA: None, pd.NaT: None, '': None})
    
    # 8. AGREGAR COLUMNA DE ARCHIVO ORIGEN
    logger.info(f"\n6. Agregando columna ARCHIVO_ORIGEN...")
    df['ARCHIVO_ORIGEN'] = nombre_archivo
    logger.info(f"   ✓ Archivo origen: {nombre_archivo}")
    
    logger.info(f"\n{'='*60}")
    logger.info(f"✓ Limpieza completada:")
    logger.info(f"  - Total de filas: {len(df)} (de {filas_iniciales} iniciales)")
    logger.info(f"  - Total de columnas: {len(df.columns)}")
    logger.info(f"{'='*60}")
    
    return df

def procesar_archivo(archivo_excel, engine, nombre_tabla):
    """
    Procesa un archivo Excel con verificación de duplicados
    
    Returns:
        Dict con resultado de la operación
    """
    nombre_archivo = os.path.basename(archivo_excel)
    
    logger.info(f"\n{'#'*60}")
    logger.info(f"PROCESANDO ARCHIVO: {nombre_archivo}")
    logger.info(f"{'#'*60}")
    
    try:
        # PASO 1: Verificar si el archivo ya fue cargado
        logger.info("\n1. Verificando si el archivo ya fue cargado...")
        info_archivo = verificar_archivo_ya_cargado(engine, nombre_archivo)
        
        accion = 'append'  # Default
        
        if info_archivo['existe']:
            logger.warning(f"⚠️  El archivo ya fue cargado ({info_archivo['total_registros']} registros)")
            
            if modo_interactivo:
                # Preguntar al usuario qué hacer
                accion = solicitar_accion_usuario(info_archivo)
                
                if accion == 'skip':
                    logger.info("✓ Archivo saltado por el usuario")
                    return {
                        'archivo': nombre_archivo,
                        'estado': 'saltado',
                        'filas': 0
                    }
            else:
                # Modo no interactivo: saltar automáticamente
                logger.info("✓ Modo no interactivo: saltando archivo duplicado")
                return {
                    'archivo': nombre_archivo,
                    'estado': 'saltado',
                    'filas': 0
                }
        else:
            logger.info("✓ Archivo nuevo, procediendo con la carga")
        
        # PASO 2: Leer la hoja FORMATO
        xls = pd.ExcelFile(archivo_excel)
        if 'FORMATO' not in xls.sheet_names:
            logger.error(f"✗ Hoja 'FORMATO' no encontrada")
            return {
                'archivo': nombre_archivo,
                'estado': 'error',
                'mensaje': "Hoja 'FORMATO' no encontrada"
            }
        
        logger.info(f"\n2. Leyendo hoja 'FORMATO'...")
        df = pd.read_excel(archivo_excel, sheet_name='FORMATO')
        logger.info(f"✓ Datos leídos: {len(df)} filas, {len(df.columns)} columnas")
        
        # PASO 3: Limpiar y preparar datos
        df = limpiar_y_preparar_datos(df, nombre_archivo)
        
        if len(df) == 0:
            logger.warning("⚠️  No hay datos válidos después de la limpieza")
            return {
                'archivo': nombre_archivo,
                'estado': 'sin_datos',
                'filas': 0
            }
        
        # PASO 4: Si es reemplazo, eliminar datos antiguos primero
        if accion == 'replace':
            logger.info(f"\n3. Eliminando datos antiguos del archivo...")
            registros_eliminados = eliminar_datos_archivo(engine, nombre_archivo)
            logger.info(f"✓ Registros eliminados: {registros_eliminados}")
            
            # Agregar FECHA_ACTUALIZACION
            df['FECHA_ACTUALIZACION'] = datetime.now()
            logger.info(f"✓ Marcando registros con FECHA_ACTUALIZACION")
        
        # PASO 5: Cargar a SQL Server
        logger.info(f"\n{'='*60}")
        logger.info(f"CARGANDO DATOS A SQL SERVER")
        logger.info(f"{'='*60}")
        logger.info(f"Tabla destino: {nombre_tabla}")
        logger.info(f"Modo: {'REEMPLAZO' if accion == 'replace' else 'AGREGAR'}")
        logger.info(f"Filas a insertar: {len(df)}")
        
        df.to_sql(
            name=nombre_tabla,
            con=engine,
            if_exists='append',  # Siempre append (ya eliminamos si era replace)
            index=False,
            chunksize=500
        )
        
        logger.info(f"\n✓ Datos cargados exitosamente a la tabla '{nombre_tabla}'")
        
        return {
            'archivo': nombre_archivo,
            'estado': 'éxito',
            'accion': accion,
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
    logger.info("ETL CALIDAD DE TRANSMISIÓN - INICIO")
    logger.info("="*60)
    logger.info(f"Modo: {'INTERACTIVO' if modo_interactivo else 'AUTOMÁTICO'}")
    
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
                nombre_tabla=nombre_tabla_sql
            )
            resultados.append(resultado)
        
        # Resumen final
        logger.info("\n" + "="*60)
        logger.info("RESUMEN FINAL")
        logger.info("="*60)
        
        exitosos = 0
        saltados = 0
        errores = 0
        sin_datos = 0
        total_filas = 0
        reemplazos = 0
        
        for resultado in resultados:
            if resultado['estado'] == 'éxito':
                accion = resultado.get('accion', 'append')
                accion_texto = '(REEMPLAZO)' if accion == 'replace' else '(AGREGADO)'
                logger.info(f"✓ {resultado['archivo']}: {resultado['filas']} filas cargadas {accion_texto}")
                exitosos += 1
                total_filas += resultado['filas']
                if accion == 'replace':
                    reemplazos += 1
            elif resultado['estado'] == 'saltado':
                logger.info(f"⊘ {resultado['archivo']}: Saltado (ya cargado previamente)")
                saltados += 1
            elif resultado['estado'] == 'sin_datos':
                logger.info(f"⊘ {resultado['archivo']}: Sin datos válidos")
                sin_datos += 1
            else:
                logger.error(f"✗ {resultado['archivo']}: ERROR - {resultado['mensaje']}")
                errores += 1
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Archivos procesados exitosamente: {exitosos}")
        logger.info(f"  - Nuevos: {exitosos - reemplazos}")
        logger.info(f"  - Reemplazados: {reemplazos}")
        logger.info(f"Archivos saltados (duplicados): {saltados}")
        logger.info(f"Archivos sin datos: {sin_datos}")
        logger.info(f"Archivos con errores: {errores}")
        logger.info(f"Total de filas cargadas: {total_filas}")
        logger.info(f"{'='*60}")
        
        # Mostrar estadísticas por archivo
        if exitosos > 0:
            logger.info("\n" + "="*60)
            logger.info("ESTADÍSTICAS POR ARCHIVO EN LA BASE DE DATOS")
            logger.info("="*60)
            try:
                with engine.connect() as conn:
                    result = conn.execute(text("EXEC sp_Estadisticas_Por_Archivo"))
                    rows = result.fetchall()
                    
                    if rows:
                        logger.info(f"\n{'Archivo':<40} {'Registros':>10} {'Primera Carga':>20}")
                        logger.info("-"*70)
                        for row in rows:
                            logger.info(f"{row[0]:<40} {row[1]:>10} {str(row[2])[:19]:>20}")
                    else:
                        logger.info("No hay estadísticas disponibles")
            except Exception as e:
                logger.warning(f"No se pudieron obtener estadísticas: {e}")
        
        logger.info(f"\n✓ Proceso completado")
        logger.info(f"📄 Log guardado en: {log_file}")
        
    except Exception as e:
        logger.error(f"\n✗ Error general: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    
    finally:
        engine.dispose()
        logger.info("\n Conexión cerrada")
