-- =============================================================================
-- SCRIPT PARA AGREGAR TABLA DE CALIDAD DE TRANSMISIÓN A BASE DE DATOS EXISTENTE
-- =============================================================================
-- Este script agrega únicamente la tabla Calidad_Transmision (hoja FORMATO)
-- a una base de datos existente
-- =============================================================================

-- IMPORTANTE: Reemplaza 'TuBaseDeDatos' con el nombre de tu base de datos
USE TuBaseDeDatos;
GO

-- =============================================================================
-- TABLA: Calidad_Transmision (datos de la hoja FORMATO)
-- =============================================================================

-- Verificar si la tabla ya existe
IF OBJECT_ID('dbo.Calidad_Transmision', 'U') IS NOT NULL
BEGIN
    PRINT '⚠️  La tabla Calidad_Transmision ya existe.';
    PRINT '    Si deseas recrearla, descomenta las siguientes líneas:';
    PRINT '    -- DROP TABLE dbo.Calidad_Transmision;';
    PRINT '';
    -- Descomentar la siguiente línea para eliminar y recrear la tabla:
    -- DROP TABLE dbo.Calidad_Transmision;
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
        
        -- Auditoría
        FECHA_INSERCION DATETIME DEFAULT GETDATE(),
        FECHA_ACTUALIZACION DATETIME NULL,
        
        -- Índices para mejorar rendimiento
        INDEX IX_Fecha_Apertura (FECHA_HORA_APERTURA),
        INDEX IX_Codigo_Elemento (CODIGO_ELEMENTO_AFECTADO),
        INDEX IX_Subestacion (SUBESTACION),
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
    
    -- Auditoría
    FECHA_INSERCION
FROM dbo.Calidad_Transmision
WHERE FECHA_HORA_APERTURA IS NOT NULL;
GO

PRINT '✓ Vista v_Resumen_Calidad_Transmision creada exitosamente';
PRINT '';
GO

-- =============================================================================
-- STORED PROCEDURE: Estadísticas Generales
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
PRINT '-- Ver últimos 10 eventos:';
PRINT 'SELECT TOP 10 * FROM dbo.v_Resumen_Calidad_Transmision ORDER BY FECHA_HORA_APERTURA DESC;';
PRINT '';
PRINT '-- Estadísticas generales:';
PRINT 'EXEC dbo.sp_Estadisticas_Transmision;';
PRINT '';
PRINT '-- Estadísticas por subestación específica:';
PRINT 'EXEC dbo.sp_Estadisticas_Transmision @Subestacion = ''LLN'';';
PRINT '';
PRINT '-- Top 10 causas de eventos:';
PRINT 'EXEC dbo.sp_Top_Causas_Eventos @Top = 10;';
PRINT '';
PRINT '-- Eventos del último mes:';
PRINT 'SELECT * FROM dbo.v_Resumen_Calidad_Transmision ';
PRINT 'WHERE FECHA_HORA_APERTURA >= DATEADD(MONTH, -1, GETDATE())';
PRINT 'ORDER BY FECHA_HORA_APERTURA DESC;';
PRINT '';
PRINT '-- Eventos prolongados (más de 4 horas):';
PRINT 'SELECT * FROM dbo.v_Resumen_Calidad_Transmision ';
PRINT 'WHERE EVENTO_PROLONGADO = ''Sí''';
PRINT 'ORDER BY DURACION_HORAS DESC;';
PRINT '';
PRINT '-- Contar registros totales:';
PRINT 'SELECT COUNT(*) AS TOTAL_EVENTOS FROM dbo.Calidad_Transmision;';
PRINT '';

-- =============================================================================
-- VERIFICACIÓN FINAL
-- =============================================================================

PRINT '========================================';
PRINT 'VERIFICACIÓN:';
PRINT '========================================';

IF OBJECT_ID('dbo.Calidad_Transmision', 'U') IS NOT NULL
    PRINT '✓ Tabla Calidad_Transmision existe';
ELSE
    PRINT '✗ ERROR: Tabla Calidad_Transmision no existe';

IF OBJECT_ID('dbo.v_Resumen_Calidad_Transmision', 'V') IS NOT NULL
    PRINT '✓ Vista v_Resumen_Calidad_Transmision existe';
ELSE
    PRINT '✗ ERROR: Vista v_Resumen_Calidad_Transmision no existe';

IF OBJECT_ID('dbo.sp_Estadisticas_Transmision', 'P') IS NOT NULL
    PRINT '✓ SP sp_Estadisticas_Transmision existe';
ELSE
    PRINT '✗ ERROR: SP sp_Estadisticas_Transmision no existe';

IF OBJECT_ID('dbo.sp_Top_Causas_Eventos', 'P') IS NOT NULL
    PRINT '✓ SP sp_Top_Causas_Eventos existe';
ELSE
    PRINT '✗ ERROR: SP sp_Top_Causas_Eventos no existe';

PRINT '';
PRINT '========================================';
PRINT '✓ ¡Script completado!';
PRINT '========================================';
PRINT '';
PRINT 'Próximos pasos:';
PRINT '1. Configura el archivo .env con tus credenciales';
PRINT '2. Ejecuta: python etl_transmision_simple.py';
PRINT '';
GO