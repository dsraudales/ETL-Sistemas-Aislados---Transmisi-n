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
    PRINT '  La tabla Calidad_Transmision ya existe.';
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
    
    PRINT ' Tabla Calidad_Transmision creada exitosamente';
    PRINT '';
END
GO
