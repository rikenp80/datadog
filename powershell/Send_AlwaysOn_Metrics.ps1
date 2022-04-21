<#
Send custom metrics to Datadog

example execution: 
#>

param
(
   $server_instance = (hostname)
)

# load assemblies
[Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Smo") | Out-Null
[Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.SMOExtended") | Out-Null


# Import the SQLPS module so that the Invoke-SQLCMD command works
Import-Module "sqlps" -DisableNameChecking


    
   
#Get replica status
$query="select rs.replica_id, rs.group_id, rs.is_local,
        rs.role,rs.role_desc,rs.operational_state,rs.operational_state_desc,
        rs.connected_state,rs.connected_state_desc,rs.recovery_health,
        rs.recovery_health_desc, rs.synchronization_health, rs.synchronization_health_desc,
        rs.last_connect_error_number, rs.last_connect_error_description,
        rs.last_connect_error_timestamp, r.replica_metadata_id,
        r.replica_server_name, r.owner_sid,r.[endpoint_url],
        r.availability_mode,r.availability_mode_desc, r.failover_mode,
        r.failover_mode_desc, r.session_timeout, r.primary_role_allow_connections,
        r.primary_role_allow_connections_desc, r.secondary_role_allow_connections,
        r.secondary_role_allow_connections_desc, r.create_date, r.modify_date,
        r.backup_priority, r.read_only_routing_url
        from sys.dm_hadr_availability_replica_states (NOLOCK) rs
        inner join sys.availability_replicas (NOLOCK) r
        on rs.replica_id=r.replica_id
        WHERE r.replica_server_name = '$server_instance'"
    
$replicas=(Invoke-Sqlcmd -ServerInstance $server_instance -database "master" -Query $query)

   

foreach($replica in $replicas)
{
    $query="Insert into AlwaysOnReplicaStatus (replica_id, group_id, is_local,
    role, role_desc, operational_state, operational_state_desc,connected_state,
    connected_state_desc, recovery_health, recovery_health_desc, synchronization_health,
    synchronization_health_desc, last_connect_error_number, last_connect_error_description,
    last_connect_error_timestamp, replica_metadata_id, replica_server_name,
    endpoint_url,availability_mode, availability_mode_desc, failover_mode, failover_mode_desc,
    session_timeout, primary_role_allow_connections, [primary_role_allow_connections_desc],
    [secondary_role_allow_connections],[secondary_role_allow_connections_desc],
    [create_date],[modify_date],[backup_priority],[read_only_routing_url],CreatedDate)
    values('"+$replica.replica_id+"', '"+$replica.group_id+"', '"+$replica.is_local+"',"`
    +$replica.role+", '"+$replica.role_desc+"', "+$replica.operational_state+", '"+$replica.operational_state_desc+"', "`
    +$replica.connected_state+", '"+$replica.connected_state_desc+"', "+$replica.recovery_health+", '"`
    +$replica.recovery_health_desc+"', "+$replica.synchronization_health+", '"`
    +$replica.synchronization_health_desc+"', "+$replica.last_connect_error_number+", '"`
    +$replica.last_connect_error_description+"', '"+$replica.last_connect_error_timestamp+"', "`
    +$replica.replica_metadata_id+", '"+$replica.replica_server_name+"', '"+$replica.endpoint_url+"', "`
    +$replica.availability_mode+", '"+$replica.availability_mode_desc+"', "+$replica.failover_mode+", '"`
    +$replica.failover_mode_desc+"', "+$replica.session_timeout+", "+$replica.primary_role_allow_connections+", '"`
    +$replica.primary_role_allow_connections_desc+"', "+$replica.secondary_role_allow_connections+", '"`
    +$replica.secondary_role_allow_connections_desc+"', '"+$replica.create_date+"', '"`
    +$replica.modify_date+"', "+$replica.backup_priority+", '"+$replica.read_only_routing_url+"', '"`
    +(Get-Date)+"')"

    #Replace empty strings with NULL
    $query=$query.replace(" ,"," NULL,")
    $query=$query.replace("'',"," NULL,")

    Invoke-Sqlcmd -ServerInstance $server_instance -database "dbManagement" -Query $query

   
    If ($replica.operational_state -eq 2)
        {C:\powershell\send_metrics.ps1 -metric sqlserver.hadr.operstate -points 1 -hostname $replica.replica_server_name}
    else
        {C:\powershell\send_metrics.ps1 -metric sqlserver.hadr.operstate -points 0 -hostname $replica.replica_server_name}
    

    C:\powershell\send_metrics.ps1 -metric sqlserver.hadr.sync_health -points $replica.synchronization_health -hostname $replica.replica_server_name

    C:\powershell\send_metrics.ps1 -metric sqlserver.hadr.connected_state -points $replica.connected_state -hostname $replica.replica_server_name    
}




#Get AlwaysOn Databases Status
$query="select ar.replica_server_name, 
    CASE
    WHEN ars.role_desc IS NULL THEN N'DISCONNECTED'
    ELSE ars.role_desc
    END as replica_role,
    CASE
    WHEN ars.is_local = 1 THEN N'LOCAL'
    ELSE 'REMOTE'
    END as is_local_desc,
    d.name as dbname,dr.database_id,dr.group_id,dr.replica_id,
    dr.group_database_id, dr.is_local, dr.synchronization_state,
    dr.synchronization_state_desc, dr.is_commit_participant,
    dr.synchronization_health,dr.synchronization_health_desc,
    dr.database_state,dr.database_state_desc,dr.is_suspended,
    dr.suspend_reason, dr.suspend_reason_desc,dr.recovery_lsn,
    dr.truncation_lsn,dr.last_sent_lsn, dr.last_sent_time,
    dr.last_received_lsn, dr.last_received_time, dr.last_hardened_lsn,
    dr.last_hardened_time, dr.last_redone_lsn, dr.log_send_queue_size,
    dr.log_send_rate, dr.redo_queue_size as redo_queue_size,dr.redo_rate, dr.filestream_send_rate,
    dr.end_of_log_lsn, dr.last_commit_lsn, dr.last_commit_time,
    dr.low_water_mark_for_ghosts
    from sys.dm_hadr_database_replica_states (NOLOCK) dr
    inner join sys.databases (NOLOCK) d
    on dr.database_id=d.database_id
    inner join sys.availability_replicas (NOLOCK) ar
    on dr.replica_id=ar.replica_id
    inner join sys.dm_hadr_availability_replica_states (NOLOCK) ars
    on ar.replica_id=ars.replica_id
    WHERE ar.replica_server_name = '$server_instance'"

$DB_AG=(Invoke-Sqlcmd -ServerInstance $server_instance -database "master" -Query $query)
    

foreach ($DB in $DB_AG)
{
    $query="Insert into AlwaysOnDbStatus([replica_server_name],
        [replica_role],
        [is_local_desc],
        [dbname],
        [database_id],
        [group_id],
        [replica_id],
        [group_database_id],
        [is_local],
        [synchronization_state],
        [synchronization_state_desc],
        [is_commit_participant],
        [synchronization_health],
        [synchronization_health_desc],
        [database_state],
        [database_state_desc],
        [is_suspended],
        [suspend_reason],
        [suspend_reason_desc],
        [recovery_lsn],
        [truncation_lsn],
        [last_sent_lsn],
        [last_sent_time],
        [last_received_lsn],
        [last_received_time],
        [last_hardened_lsn],
        [last_hardened_time],
        [last_redone_lsn],
        [last_redone_time],
        [log_send_queue_size],
        [log_send_rate],
        [redo_queue_size],
        [redo_rate],
        [filestream_send_rate],
        [end_of_log_lsn],
        [last_commit_lsn],
        [last_commit_time],
        [low_water_mark_for_ghosts],
        [CreatedDate])
        values('"+$DB.replica_server_name+"', '"`
        +$DB.replica_role+"', '"`
        +$DB.is_local_desc+"', '"`
        +$DB.dbname+"', "`
        +$DB.database_id+", '"`
        +$DB.group_id+"', '"`
        +$DB.replica_id+"', '"`
        +$DB.group_database_id+"', '"`
        +$DB.is_local+"', "`
        +$DB.synchronization_state+", '"`
        +$DB.synchronization_state_desc+"', '"`
        +$DB.is_commit_participant+"', "`
        +$DB.synchronization_health+", '"`
        +$DB.synchronization_health_desc+"', "`
        +$DB.database_state+", '"`
        +$DB.database_state_desc+"', '"`
        +$DB.is_suspended+"', "`
        +$DB.suspend_reason+", '"`
        +$DB.suspend_reason_desc+"', "`
        +$DB.recovery_lsn+", "`
        +$DB.truncation_lsn+", "`
        +$DB.last_sent_lsn+", '"`
        +$DB.last_sent_time+"', "`
        +$DB.last_received_lsn+", '"`
        +$DB.last_received_time+"', "`
        +$DB.last_hardened_lsn+", '"`
        +$DB.last_hardened_time+"', "`
        +$DB.last_redone_lsn+", '"`
        +$DB.last_redone_time+"', "`
        +$DB.log_send_queue_size+", "`
        +$DB.log_send_rate+", "`
        +$DB.redo_queue_size+", "`
        +$DB.redo_rate+", "`
        +$DB.filestream_send_rate+", "`
        +$DB.end_of_log_lsn+", "`
        +$DB.last_commit_lsn+", '"`
        +$DB.last_commit_time+"', "`
        +$DB.low_water_mark_for_ghosts+", '"`
        +(get-date)+"')"

    #Replace empty strings with NULL
    $query=$query.replace(" ,"," NULL,")
    $query=$query.replace("'',"," NULL,")
   
    Invoke-Sqlcmd -ServerInstance $server_instance -database "dbManagement" -Query $query
       
             

    $tags="database:"+$DB.replica_server_name+"."+$DB.dbname

    $last_commit_secs = [math]::Round((New-TimeSpan $DB.last_commit_time (Get-Date)).TotalSeconds)
    
    C:\powershell\send_metrics.ps1 -metric sqlserver.hadr.last_commit_time -points $last_commit_secs -hostname $DB.replica_server_name -tags $tags

    C:\powershell\send_metrics.ps1 -metric sqlserver.hadr.db_synchronization_state -points $DB.synchronization_state -hostname $DB.replica_server_name -tags $tags

    C:\powershell\send_metrics.ps1 -metric sqlserver.hadr.db_synchronization_health -points $DB.synchronization_health -hostname $DB.replica_server_name -tags $tags

    C:\powershell\send_metrics.ps1 -metric sqlserver.hadr.database_state -points $DB.database_state -hostname $DB.replica_server_name -tags $tags
   
} 