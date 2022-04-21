CREATE LOGIN datadog WITH PASSWORD = 'YOUR_PASSWORD', CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF;
    CREATE USER datadog FOR LOGIN datadog;
    GRANT SELECT on sys.dm_os_performance_counters to datadog;
    GRANT VIEW SERVER STATE to datadog;
    GRANT VIEW ANY DEFINITION to datadog;