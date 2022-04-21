param
(
  $server_instance = (hostname)
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
    Import-Module "sqlps" -DisableNameChecking
}



$tags="role:database"",""app:sqlserver"""


#run query to get data
$query =

    "
    CREATE TABLE #max_job_run_requested_date
    (
	    run_requested_date DATETIME,
	    job_id UNIQUEIDENTIFIER
    )

    INSERT INTO #max_job_run_requested_date
    SELECT  MAX(run_requested_date), job_id FROM msdb.dbo.sysjobactivity group by job_id


    SELECT  t.run_requested_date,
		    a.stop_execution_date,
		    t.job_id,
		    j.name 'job_name',
            c.name 'category',
		    CASE WHEN a.stop_execution_date IS NOT NULL THEN 1 ELSE 0 END 'recent_job_stop'
    FROM [msdb].[dbo].[syscategories] c
		    inner join msdb.dbo.sysjobs j ON c.category_id = j. category_id 
		    inner join #max_job_run_requested_date t ON j.job_id = t.job_id
		    LEFT join msdb.dbo.sysjobactivity a ON t.job_id = a.job_id AND t.run_requested_date = a.run_requested_date
    WHERE c.name in ('REPL-LogReader', 'REPL-Distribution')
    "


$results=(Invoke-Sqlcmd -ServerInstance $server_instance -database "msdb" -Query $query)


foreach($result in $results)
{
    $current_tags = $tags + ",""recent_job_stop:" + $result.recent_job_stop + """,""job_name:" + $result.job_name + """,""category:" + $result.category + """,""stop_execution_date:" + $result.stop_execution_date + """,""run_requested_date:" + $result.run_requested_date + ""

    $current_tags=$current_tags.Replace("\","\\")          

    C:\powershell\send_metrics.ps1 -metric sqlserver.repl.job_stop -points $result.recent_job_stop -hostname $(hostname.exe) -tags $current_tags
}
