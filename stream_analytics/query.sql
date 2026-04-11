-- =============================================================================
-- Rideau Canal Skateway – Azure Stream Analytics Query
-- =============================================================================
-- Inputs  : [rideau-canal-iot-input]    – Azure IoT Hub
-- Outputs : [cosmosdb-output] – Azure Cosmos DB 
--           [blobstorage-output]     – Azure Blob
--
-- Window  : 5-minute non-overlapping TumblingWindow, grouped by location.
--
-- Safety Status Logic (applied per window):
--   Safe    : MIN(ice_thickness) >= 30 cm  AND MAX(surface_temperature) <= -2 °C
--   Caution : MIN(ice_thickness) >= 25 cm  AND MAX(surface_temperature) <=  0 °C
--   Unsafe  : all other conditions
-- =============================================================================



-- Output 1 -> Storing in Cosmos DB 

SELECT
    -- Document identity
    IoTHub.ConnectionDeviceId AS DeviceId,
    location,
    System.Timestamp()                       AS window_end_time,

    -- Ice thickness (cm) – primary safety indicator
    AVG(CAST(ice_thickness AS float))        AS avg_ice_thickness,
    MIN(CAST(ice_thickness AS float))        AS min_ice_thickness,
    MAX(CAST(ice_thickness AS float))        AS max_ice_thickness,

    -- Surface temperature (°C) – melting risk
    AVG(CAST(surface_temperature AS float))  AS avg_surface_temperature,
    MIN(CAST(surface_temperature AS float))  AS min_surface_temperature,
    MAX(CAST(surface_temperature AS float))  AS max_surface_temperature,

    -- Snow accumulation (cm)
    AVG(CAST(snow_accumulation AS float))    AS avg_snow_accumulation,
    MAX(CAST(snow_accumulation AS float))    AS max_snow_accumulation,

    -- External (air) temperature (°C)
    AVG(CAST(external_temperature AS float)) AS avg_external_temperature,
    MIN(CAST(external_temperature AS float)) AS min_external_temperature,

    -- Reading count
    COUNT(*)                                 AS reading_count,

    -- Safety status – evaluated against worst-case values in the window
    CASE
        WHEN MIN(CAST(ice_thickness AS float)) >= 30
         AND MAX(CAST(surface_temperature AS float)) <= -2
        THEN 'Safe'

        WHEN MIN(CAST(ice_thickness AS float)) >= 25
         AND MAX(CAST(surface_temperature AS float)) <= 0
        THEN 'Caution'

        ELSE 'Unsafe'
    END AS safety_status

INTO
    [cosmosdb-output]
FROM
    [iothub-input]
GROUP BY
    location,
    IoTHub.ConnectionDeviceId, TumblingWindow(minute, 5)



-- Output 2 -> Storing in Blob Storage 

SELECT
    IoTHub.ConnectionDeviceId AS DeviceId,
    location,
    System.Timestamp()                       AS window_end_time,

    AVG(CAST(ice_thickness AS float))        AS avg_ice_thickness,
    MIN(CAST(ice_thickness AS float))        AS min_ice_thickness,
    MAX(CAST(ice_thickness AS float))        AS max_ice_thickness,

    AVG(CAST(surface_temperature AS float))  AS avg_surface_temperature,
    MIN(CAST(surface_temperature AS float))  AS min_surface_temperature,
    MAX(CAST(surface_temperature AS float))  AS max_surface_temperature,

    AVG(CAST(snow_accumulation AS float))    AS avg_snow_accumulation,
    MAX(CAST(snow_accumulation AS float))    AS max_snow_accumulation,

    AVG(CAST(external_temperature AS float)) AS avg_external_temperature,
    MIN(CAST(external_temperature AS float)) AS min_external_temperature,

    COUNT(*)                                 AS reading_count,

    CASE
        WHEN MIN(CAST(ice_thickness AS float)) >= 30
         AND MAX(CAST(surface_temperature AS float)) <= -2
        THEN 'Safe'

        WHEN MIN(CAST(ice_thickness AS float)) >= 25
         AND MAX(CAST(surface_temperature AS float)) <= 0
        THEN 'Caution'

        ELSE 'Unsafe'
    END AS safety_status

INTO
    [blob-output]
FROM
    [iothub-input]
GROUP BY
    location,
    IoTHub.ConnectionDeviceId, TumblingWindow(minute, 5)
