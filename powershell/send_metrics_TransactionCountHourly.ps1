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


#Get Transactional_Count_Hourly info
         $query="Select Threshold_Transactions_count from Transaction_count_ForDD"
         $TH=(Invoke-Sqlcmd -ServerInstance $server_instance -database "dbManagement" -Query $query)
write-output $query

if ($TH.Threshold_Transactions_count -ge 0)
{
         function sendMetrics($metric, $points)
         {
         $tags="host:"+$env:computername
    
         C:\powershell\send_metrics.ps1 -metric $metric -points $points -hostname $env:computername -tags $tags
         echo $tags
         }
         sendMetrics -metric "sqlserver.TransactioCountThreshold"  -points $TH.Threshold_Transactions_count
}