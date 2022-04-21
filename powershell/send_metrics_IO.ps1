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


#Get IO info
         $query="EXEC GetDatadogMetrics"
         $IOs=(Invoke-Sqlcmd -ServerInstance $server_instance -database "dbManagement" -Query $query)
write-output $query
foreach($IO in $IOs)
{

         function sendMetrics($metric, $points)
         {
    
         C:\powershell\send_metrics.ps1 -metric $metric -points $points -hostname $env:computername -tags $IO.tags
         echo $tags
         }
         sendMetrics -metric $IO.metric  -points $IO.value
}
$disks = Get-WmiObject -Query "Select * from Win32_perfformatteddata_perfdisk_LogicalDisk"
foreach($disk in $disks)
{
         
         function sendMetrics($metric, $points)
         {
         $d=$disk.Name.Substring(0,1)
         $d=$d -replace "_","Total"
         $tags="instance:$d,hostname:$env:computername,team:CDO"
         C:\powershell\send_metrics.ps1 -metric $metric -points $points -hostname $env:computername -tags $IO.tags
         echo $tags
         }


$value= $disk.CurrentDiskQueueLength

         sendMetrics -metric "SQLServer.FileIO.CurrentDiskQueueLength"  -points $value
}