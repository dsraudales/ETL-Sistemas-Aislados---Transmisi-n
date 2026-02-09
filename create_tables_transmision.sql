-- =============================================================================
-- SCRIPT PARA AGREGAR TABLA DE CALIDAD DE TRANSMISIÓN
-- =============================================================================
-- =============================================================================
-- TABLA: Calidad_Transmision
-- =============================================================================

-- Verificar si la tabla ya existe
IF OBJECT_ID('dbo.Calidad_Transmision', 'U') IS NOT NULL
BEGIN
    PRINT '⚠️  La tabla Calidad_Transmision ya existe.';
    PRINT '    Verificando si necesita agregar columnas nuevas...';
    
    -- Agregar columna ARCHIVO_ORIGEN si no existe
    IF NOT EXISTS (
        SELECT 1 FROM sys.columns 
        WHERE object_id = OBJECT_ID('dbo.Calidad_Transmision') 
        AND name = 'ARCHIVO_ORIGEN'
    )
    BEGIN
        ALTER TABLE dbo.Calidad_Transmision
        ADD ARCHIVO_ORIGEN NVARCHAR(255) NULL;
        PRINT '    ✓ Columna ARCHIVO_ORIGEN agregada';
    END
    ELSE
        PRINT '    ✓ Columna ARCHIVO_ORIGEN ya existe';
    
    -- Agregar índice en ARCHIVO_ORIGEN si no existe
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes 
        WHERE object_id = OBJECT_ID('dbo.Calidad_Transmision') 
        AND name = 'IX_Archivo_Origen'
    )
    BEGIN
        CREATE INDEX IX_Archivo_Origen ON dbo.Calidad_Transmision(ARCHIVO_ORIGEN);
        PRINT '    ✓ Índice IX_Archivo_Origen creado';
    END
    ELSE
        PRINT '    ✓ Índice IX_Archivo_Origen ya existe';
    
    PRINT '';
END
GO
-- Crear la tabla si no existe
IF OBJECT_ID('dbo.Calidad_Transmision', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Calidad_Transmision (
        -- Identificador único
        ID INT IDENTITY(1,1) PRIMARY KEY,
        
        -- Fechas del evento
        FECHA_HORA_APERTURA DATETIME NULL,
        FECHA_HORA_CIERRE DATETIME NULL,
        
        -- Métricas del evento
        DURACION_INDISPONIBILIDAD_MINUTOS DECIMAL(18,2) NULL,
        CARGA_MEGAS DECIMAL(18,2) NULL,
        
        -- Identificación del elemento
        CODIGO_ELEMENTO_AFECTADO NVARCHAR(50) NULL,
        TIPO_EQUIPO NVARCHAR(100) NULL,
        CIRCUITOS_AFECTADOS NVARCHAR(255) NULL,
        
        -- Ubicación
        SUBESTACION NVARCHAR(100) NULL,
        REGION NVARCHAR(100) NULL,
        
        -- Detalles técnicos
        CODIGO_INTERRUPTOR NVARCHAR(50) NULL,
        NIVEL_DE_TENSION NVARCHAR(50) NULL,
        PROTECCION_OPERADA NVARCHAR(255) NULL,
        
        -- Clasificación del evento
        ORIGEN_INDISPONIBILIDAD NVARCHAR(100) NULL,
        CAUSA_EVENTO NVARCHAR(255) NULL,
        EXCEPCIONES NVARCHAR(255) NULL,
        TIPO_INDISPONIBILIDAD NVARCHAR(100) NULL,
        TIPO_MANTENIMIENTO NVARCHAR(100) NULL,
        
        -- Descripción
        DESCRIPCION_EVENTO NVARCHAR(MAX) NULL,
        
        -- Control de archivos origen y auditoría
        ARCHIVO_ORIGEN NVARCHAR(255) NULL,
        FECHA_INSERCION DATETIME DEFAULT GETDATE(),
        FECHA_ACTUALIZACION DATETIME NULL,
        
        -- Índices para mejorar rendimiento
        INDEX IX_Fecha_Apertura (FECHA_HORA_APERTURA),
        INDEX IX_Codigo_Elemento (CODIGO_ELEMENTO_AFECTADO),
        INDEX IX_Subestacion (SUBESTACION),
        INDEX IX_Archivo_Origen (ARCHIVO_ORIGEN),
        INDEX IX_Fecha_Insercion (FECHA_INSERCION)
    );
    
    PRINT '✓ Tabla Calidad_Transmision creada exitosamente';
    PRINT '';
END
GO

-- =============================================================================
-- VISTA: Resumen de Eventos con Cálculos
-- =============================================================================

IF OBJECT_ID('dbo.v_Resumen_Calidad_Transmision', 'V') IS NOT NULL
    DROP VIEW dbo.v_Resumen_Calidad_Transmision;
GO

CREATE VIEW dbo.v_Resumen_Calidad_Transmision AS
SELECT 
    ID,
    FECHA_HORA_APERTURA,
    FECHA_HORA_CIERRE,
    
    -- Duraciones
    DURACION_INDISPONIBILIDAD_MINUTOS,
    CAST(DURACION_INDISPONIBILIDAD_MINUTOS / 60.0 AS DECIMAL(10,2)) AS DURACION_HORAS,
    
    -- Carga
    CARGA_MEGAS,
    
    -- Identificación
    CODIGO_ELEMENTO_AFECTADO,
    TIPO_EQUIPO,
    CIRCUITOS_AFECTADOS,
    
    -- Ubicación
    SUBESTACION,
    REGION,
    
    -- Técnico
    CODIGO_INTERRUPTOR,
    NIVEL_DE_TENSION,
    PROTECCION_OPERADA,
    
    -- Clasificación
    ORIGEN_INDISPONIBILIDAD,
    CAUSA_EVENTO,
    EXCEPCIONES,
    TIPO_INDISPONIBILIDAD,
    TIPO_MANTENIMIENTO,
    
    -- Descripción
    DESCRIPCION_EVENTO,
    
    -- Indicadores
    CASE 
        WHEN DURACION_INDISPONIBILIDAD_MINUTOS > 240 THEN 'Sí' 
        ELSE 'No' 
    END AS EVENTO_PROLONGADO,
    
    -- Fecha y hora
    YEAR(FECHA_HORA_APERTURA) AS ANIO,
    MONTH(FECHA_HORA_APERTURA) AS MES,
    DATENAME(MONTH, FECHA_HORA_APERTURA) AS NOMBRE_MES,
    DATEPART(WEEK, FECHA_HORA_APERTURA) AS SEMANA,
    DATENAME(WEEKDAY, FECHA_HORA_APERTURA) AS DIA_SEMANA,
    
    -- Control de origen y auditoría
    ARCHIVO_ORIGEN,
    FECHA_INSERCION,
    FECHA_ACTUALIZACION
FROM dbo.Calidad_Transmision
WHERE FECHA_HORA_APERTURA IS NOT NULL;
GO

PRINT '✓ Vista v_Resumen_Calidad_Transmision creada exitosamente';
PRINT '';
GO

-- =============================================================================
-- STORED PROCEDURE: Verificar si archivo ya fue cargado
-- =============================================================================

IF OBJECT_ID('dbo.sp_Verificar_Archivo_Cargado', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_Verificar_Archivo_Cargado;
GO

CREATE PROCEDURE dbo.sp_Verificar_Archivo_Cargado
    @NombreArchivo NVARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        COUNT(*) AS Total_Registros,
        MIN(FECHA_INSERCION) AS Primera_Carga,
        MAX(FECHA_INSERCION) AS Ultima_Carga,
        MAX(FECHA_ACTUALIZACION) AS Ultima_Actualizacion,
        MIN(FECHA_HORA_APERTURA) AS Evento_Mas_Antiguo,
        MAX(FECHA_HORA_APERTURA) AS Evento_Mas_Reciente
    FROM dbo.Calidad_Transmision
    WHERE ARCHIVO_ORIGEN = @NombreArchivo;
END;
GO

PRINT '✓ SP sp_Verificar_Archivo_Cargado creado exitosamente';
PRINT '';
GO

-- =============================================================================
-- STORED PROCEDURE: Eliminar datos de un archivo específico
-- =============================================================================

IF OBJECT_ID('dbo.sp_Eliminar_Datos_Archivo', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_Eliminar_Datos_Archivo;
GO

CREATE PROCEDURE dbo.sp_Eliminar_Datos_Archivo
    @NombreArchivo NVARCHAR(255),
    @RegistrosEliminados INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    DELETE FROM dbo.Calidad_Transmision
    WHERE ARCHIVO_ORIGEN = @NombreArchivo;
    
    SET @RegistrosEliminados = @@ROWCOUNT;
END;
GO

PRINT '✓ SP sp_Eliminar_Datos_Archivo creado exitosamente';
PRINT '';
GO

-- =============================================================================
-- STORED PROCEDURE: Estadísticas por archivo
-- =============================================================================

IF OBJECT_ID('dbo.sp_Estadisticas_Por_Archivo', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_Estadisticas_Por_Archivo;
GO

CREATE PROCEDURE dbo.sp_Estadisticas_Por_Archivo
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        ARCHIVO_ORIGEN,
        COUNT(*) AS Total_Registros,
        MIN(FECHA_INSERCION) AS Primera_Carga,
        MAX(FECHA_INSERCION) AS Ultima_Carga,
        MAX(FECHA_ACTUALIZACION) AS Ultima_Actualizacion,
        COUNT(CASE WHEN FECHA_ACTUALIZACION IS NOT NULL THEN 1 END) AS Registros_Actualizados,
        MIN(FECHA_HORA_APERTURA) AS Evento_Mas_Antiguo,
        MAX(FECHA_HORA_APERTURA) AS Evento_Mas_Reciente,
        SUM(DURACION_INDISPONIBILIDAD_MINUTOS) / 60.0 AS Total_Horas_Indisponibilidad
    FROM dbo.Calidad_Transmision
    WHERE ARCHIVO_ORIGEN IS NOT NULL
    GROUP BY ARCHIVO_ORIGEN
    ORDER BY MAX(FECHA_INSERCION) DESC;
END;
GO

PRINT '✓ SP sp_Estadisticas_Por_Archivo creado exitosamente';
PRINT '';
GO

-- =============================================================================
-- STORED PROCEDURE: Estadísticas Generales (versión mejorada)
-- =============================================================================

IF OBJECT_ID('dbo.sp_Estadisticas_Transmision', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_Estadisticas_Transmision;
GO

CREATE PROCEDURE dbo.sp_Estadisticas_Transmision
    @FechaInicio DATETIME = NULL,
    @FechaFin DATETIME = NULL,
    @Subestacion NVARCHAR(100) = NULL,
    @Region NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Si no se especifican fechas, usar último año
    IF @FechaInicio IS NULL
        SET @FechaInicio = DATEADD(YEAR, -1, GETDATE());
    
    IF @FechaFin IS NULL
        SET @FechaFin = GETDATE();
    
    SELECT 
        -- Agrupación
        COALESCE(SUBESTACION, 'Sin Subestación') AS SUBESTACION,
        COALESCE(REGION, 'Sin Región') AS REGION,
        
        -- Conteos
        COUNT(*) AS TOTAL_EVENTOS,
        COUNT(CASE WHEN TIPO_INDISPONIBILIDAD = 'Forzada' THEN 1 END) AS EVENTOS_FORZADOS,
        COUNT(CASE WHEN TIPO_INDISPONIBILIDAD = 'Programada' THEN 1 END) AS EVENTOS_PROGRAMADOS,
        
        -- Duraciones
        SUM(DURACION_INDISPONIBILIDAD_MINUTOS) AS TOTAL_MINUTOS,
        CAST(SUM(DURACION_INDISPONIBILIDAD_MINUTOS) / 60.0 AS DECIMAL(10,2)) AS TOTAL_HORAS,
        AVG(DURACION_INDISPONIBILIDAD_MINUTOS) AS PROMEDIO_MINUTOS_EVENTO,
        MAX(DURACION_INDISPONIBILIDAD_MINUTOS) AS MAX_MINUTOS_EVENTO,
        
        -- Carga
        SUM(CARGA_MEGAS) AS TOTAL_CARGA_MW,
        AVG(CARGA_MEGAS) AS PROMEDIO_CARGA_MW,
        MAX(CARGA_MEGAS) AS MAX_CARGA_MW,
        
        -- Elementos
        COUNT(DISTINCT CODIGO_ELEMENTO_AFECTADO) AS ELEMENTOS_AFECTADOS
        
    FROM dbo.Calidad_Transmision
    WHERE 
        FECHA_HORA_APERTURA BETWEEN @FechaInicio AND @FechaFin
        AND (@Subestacion IS NULL OR SUBESTACION = @Subestacion)
        AND (@Region IS NULL OR REGION = @Region)
    GROUP BY SUBESTACION, REGION
    ORDER BY TOTAL_HORAS DESC;
END;
GO

PRINT '✓ Stored Procedure sp_Estadisticas_Transmision creado exitosamente';
PRINT '';
GO

-- =============================================================================
-- STORED PROCEDURE: Top Causas de Eventos
-- =============================================================================

IF OBJECT_ID('dbo.sp_Top_Causas_Eventos', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_Top_Causas_Eventos;
GO

CREATE PROCEDURE dbo.sp_Top_Causas_Eventos
    @Top INT = 10,
    @FechaInicio DATETIME = NULL,
    @FechaFin DATETIME = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @FechaInicio IS NULL
        SET @FechaInicio = DATEADD(YEAR, -1, GETDATE());
    
    IF @FechaFin IS NULL
        SET @FechaFin = GETDATE();
    
    SELECT TOP (@Top)
        CAUSA_EVENTO,
        COUNT(*) AS TOTAL_EVENTOS,
        SUM(DURACION_INDISPONIBILIDAD_MINUTOS) / 60.0 AS TOTAL_HORAS,
        AVG(DURACION_INDISPONIBILIDAD_MINUTOS) AS PROMEDIO_MINUTOS,
        SUM(CARGA_MEGAS) AS TOTAL_CARGA_MW
    FROM dbo.Calidad_Transmision
    WHERE 
        FECHA_HORA_APERTURA BETWEEN @FechaInicio AND @FechaFin
        AND CAUSA_EVENTO IS NOT NULL
    GROUP BY CAUSA_EVENTO
    ORDER BY TOTAL_EVENTOS DESC;
END;
GO

PRINT '✓ Stored Procedure sp_Top_Causas_Eventos creado exitosamente';
PRINT '';
GO

-- =============================================================================
-- CONSULTAS DE EJEMPLO
-- =============================================================================

PRINT '';
PRINT '========================================';
PRINT 'CONSULTAS DE EJEMPLO:';
PRINT '========================================';
PRINT '';
PRINT '-- Verificar si un archivo ya fue cargado:';
PRINT 'EXEC sp_Verificar_Archivo_Cargado @NombreArchivo = ''TRANSMISION_ENE_2024.xlsx'';';
PRINT '';
PRINT '-- Ver estadísticas por archivo:';
PRINT 'EXEC sp_Estadisticas_Por_Archivo;';
PRINT '';
PRINT '-- Ver últimos 10 eventos:';
PRINT 'SELECT TOP 10 * FROM v_Resumen_Calidad_Transmision ORDER BY FECHA_HORA_APERTURA DESC;';
PRINT '';
PRINT '-- Ver archivos únicos cargados:';
PRINT 'SELECT DISTINCT ARCHIVO_ORIGEN, COUNT(*) AS Total_Registros';
PRINT 'FROM Calidad_Transmision';
PRINT 'GROUP BY ARCHIVO_ORIGEN';
PRINT 'ORDER BY MAX(FECHA_INSERCION) DESC;';
PRINT '';
PRINT '-- Eliminar datos de un archivo específico:';
PRINT 'DECLARE @Eliminados INT;';
PRINT 'EXEC sp_Eliminar_Datos_Archivo @NombreArchivo = ''archivo.xlsx'', @RegistrosEliminados = @Eliminados OUTPUT;';
PRINT 'SELECT @Eliminados AS Registros_Eliminados;';
PRINT '';

-- =============================================================================
-- VERIFICACIÓN FINAL
-- =============================================================================

PRINT '========================================';
PRINT 'VERIFICACIÓN:';
PRINT '========================================';

IF OBJECT_ID('dbo.Calidad_Transmision', 'U') IS NOT NULL
BEGIN
    PRINT '✓ Tabla Calidad_Transmision existe';
    
    -- Verificar columna ARCHIVO_ORIGEN
    IF EXISTS (
        SELECT 1 FROM sys.columns 
        WHERE object_id = OBJECT_ID('dbo.Calidad_Transmision') 
        AND name = 'ARCHIVO_ORIGEN'
    )
        PRINT '✓ Columna ARCHIVO_ORIGEN existe';
    ELSE
        PRINT '✗ ERROR: Columna ARCHIVO_ORIGEN no existe';
END
ELSE
    PRINT '✗ ERROR: Tabla Calidad_Transmision no existe';

IF OBJECT_ID('dbo.v_Resumen_Calidad_Transmision', 'V') IS NOT NULL
    PRINT '✓ Vista v_Resumen_Calidad_Transmision existe';

IF OBJECT_ID('dbo.sp_Verificar_Archivo_Cargado', 'P') IS NOT NULL
    PRINT '✓ SP sp_Verificar_Archivo_Cargado existe';

IF OBJECT_ID('dbo.sp_Eliminar_Datos_Archivo', 'P') IS NOT NULL
    PRINT '✓ SP sp_Eliminar_Datos_Archivo existe';

IF OBJECT_ID('dbo.sp_Estadisticas_Por_Archivo', 'P') IS NOT NULL
    PRINT '✓ SP sp_Estadisticas_Por_Archivo existe';

