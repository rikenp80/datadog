<#
Send custom metrics to Datadog

example execution: 
#>

param
(
$server_instance=$env.computername
)
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


#connect to sql server
        $Server_SQL = New-Object Microsoft.SqlServer.Management.Smo.Server ($server_instance)
		$Server_SQL.ConnectionContext.StatementTimeout = 0


#get replication snapshot job status 
$query="CREATE TABLE #JOBIDRS(
	[session_id] [varchar](100) NULL,
	[jobid] [varchar](100) NOT NULL
) 
--select * from msdb.dbo.syscategories 
insert into #JOBIDRS
select  MAX(session_id) , job_id  
from  msdb.dbo.sysjobactivity group by job_id

SELECT  T3.stop_execution_date , T4.jobid , T4.session_id , T2.name, T3.next_scheduled_run_date,T3.start_execution_date,
 CASE WHEN T3.stop_execution_date IS NULL and T3.start_execution_date IS NOT NULL THEN 1 ELSE 0 END 'recent_job_start'

FROM
	[msdb].[dbo].[syscategories] T1
		join  msdb.dbo.sysjobs T2 on T1.category_id = T2. category_id 
		join #JOBIDRS T4 on T2.job_id = T4.jobid
		LEFT join msdb.dbo.sysjobactivity T3 on T4.jobid = T3.job_id AND T4.session_id = T3.session_id
		where  T1.name = 'REPL-Snapshot'"
$results=(Invoke-Sqlcmd -ServerInstance $server_instance -database "msdb" -Query $query)



foreach($result in $results)
{
   #$current_tags = $tags + ",""start_execution_date:" + $result.start_execution_date +  """,""name:" + $result.name + ""
  $current_tags = $tags + """,""recent_job_start:" + $result.recent_job_start + """,""name:" + $result.name +""
  $current_tags=$current_tags.Replace("\","\\")
  C:\powershell\send_metrics.ps1 -metric sqlserver.REPL_Snapshot_Started -points $result.recent_job_start -hostname $env:COMPUTERNAME -tags $current_tags

}





