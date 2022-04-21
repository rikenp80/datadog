# load assemblies
[Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Smo") | Out-Null
[Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.SMOExtended") | Out-Null


# Import the SQLPS module so that the Invoke-SQLCMD command works
if ($PSVersionTable.PSVersion.Major -le 2)  
{if ( (Get-PSSnapin -Name SqlServerCmdletSnapin100 -ErrorAction SilentlyContinue) -eq $null )
    {Add-PsSnapin SqlServerCmdletSnapin100}

if ( (Get-PSSnapin -Name SqlServerProviderSnapin100 -ErrorAction SilentlyContinue) -eq $null )
    {Add-PsSnapin SqlServerProviderSnapin100}
    }
else
{
    # Import the SQLPS module so that the Invoke-SQLCMD command works
    Import-Module “sqlps” -DisableNameChecking
}



$tags="role:database"",""app:sqlserver"""


#connect to sql server
$Server_SQL = New-Object Microsoft.SqlServer.Management.Smo.Server ($server_instance)
$Server_SQL.ConnectionContext.StatementTimeout = 0



#get running queries
$query =

    "
    --Delete old data
    DELETE FROM LongRunningSessions WHERE elapsedtime_sec = 0 and createdate < dateadd(d, -2, getdate())

    --Gather all currently running sessions
    DECLARE @LongRunningSessions TABLE (session_id smallint, command nvarchar(32), source_host nvarchar(128), login_name nvarchar(128), elapsedtime_sec int, db nvarchar(128))
    INSERT INTO @LongRunningSessions
    SELECT	r.session_id
	    ,	r.command
	    ,	e.host_name
	    ,	e.login_name
	    ,	r.total_elapsed_time/1000
	    ,	DB_Name(r.database_id)
    FROM sys.dm_exec_requests r
	    LEFT JOIN sys.dm_exec_sessions e ON r.session_id = e.session_id
    WHERE r.session_id > 50 and command <> 'BACKUP DATABASE'


    --If a session is still running then update the elapsed time
    UPDATE l
    SET l.elapsedtime_sec = t.elapsedtime_sec, l.updatedate = getdate()
    FROM @LongRunningSessions t INNER JOIN LongRunningSessions l
		    ON l.session_id = t.session_id
		    AND l.command = t.command
		    AND l.source_host = t.source_host
		    AND l.login_name = t.login_name
		    AND l.db = t.db

    
    --If the session is no longer running, set the elapsed time to 0
    UPDATE l
    SET l.elapsedtime_sec = 0, l.updatedate = getdate()
    FROM LongRunningSessions l
    WHERE NOT EXISTS (	
					    SELECT * FROM @LongRunningSessions t
					    WHERE l.session_id = t.session_id
						    AND l.command = t.command
						    AND l.source_host = t.source_host
						    AND l.login_name = t.login_name
						    AND l.db = t.db 
				     )	

    
    --If the session does not exist, insert a new row
    INSERT INTO LongRunningSessions (session_id, command, source_host, login_name, elapsedtime_sec, db)
    SELECT * FROM @LongRunningSessions t WHERE NOT EXISTS	(
														    SELECT * FROM LongRunningSessions l
														    WHERE l.session_id = t.session_id
															    AND l.command = t.command
															    AND l.source_host = t.source_host
															    AND l.login_name = t.login_name
															    AND l.db = t.db
														    )

    --return data
    select * from LongRunningSessions
    "


$results=(Invoke-Sqlcmd -ServerInstance $server_instance -database "dbManagement" -Query $query)


foreach($result in $results)
{
    $current_tags = $tags + ",""elapsedtime_sec:" + $result.elapsedtime_sec + """,""db:" + $result.db + """,""command:" + $result.command + """,""source_host:" + $result.source_host + """,""login_name:" + $result.login_name + """,""session_id:" + $result.session_id + ""

    $current_tags=$current_tags.Replace("\","\\")          

    C:\powershell\send_metrics.ps1 -metric sqlserver.queryduration -points $result.elapsedtime_sec -hostname $env:COMPUTERNAME -tags $current_tags
}


