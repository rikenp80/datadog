<#
$counters = `
'
\SQLServer:Databases(*)\Write Transactions/sec,
\SQLServer:Databases(*)\Transactions/sec,
\SQLServer:SQL Statistics\Batch Requests/sec,
\SQLServer:General Statistics\Processes blocked,
\PhysicalDisk(*)\Avg. Disk sec/Read,
\PhysicalDisk(*)\Avg. Disk sec/Write
'

while ($true)
{
    C:\Powershell\send_metrics_Perfmon.ps1 -metric_group 'sqlserver' -sample_interval 1 -max_samples 60 -counters $counters
}
#>

param
(
  $counters,
  $metric_group,
  $sample_interval = 1, #number of seconds between data samples 
  $max_samples = 1 #number of samples to get

)

#set initial tags for use in datadog
$initial_tags="role:database"",""app:sqlserver"""


#get data for counter
$counters = $counters.Replace("`r`n","")
$counters_split = $counters -split","
$counters_arr = @($counters_split)

$counter_results = Get-Counter -Counter $counters_arr -MaxSamples $max_samples -SampleInterval $sample_interval

$counter_samples = $counter_results.CounterSamples | Where-Object { $_.InstanceName -notin("model","master", "msdb", "mssqlsystemresource")}
 

#get average of values per instance
$average_values = $counter_samples | Group-Object -Property Path, InstanceName |
            ForEach-Object {$_ | Select-Object -Property Name, @{n='Average';e={(($_.Group | Measure-Object -Property CookedValue -Average).Average)}};}
    

#$average_values

#send metric and values to datadog
foreach($row in $average_values)
{    
#extract counter name from input variable
    $counter = $row.name.split(",")[0]
    $counter = $counter.split("\")[-1].ToLower()

    
    if ($instance -eq "")
        {$instance = "_total"}
    else
        {$instance = $row.name.split(",")[1]}

    $value = $row.Average
       
    
    #set metric name for use in datadog
    $metric = $metric_group + "." + $counter.Replace(".", "")
    $metric = $metric.Replace(" ", "_")
    $metric = $metric.Replace("/", "_")    
    $metric = $metric.Replace("%", "pct") 
    
    $current_tags = $initial_tags + ",""instance:" + $instance + ""
    $current_tags=$current_tags.Replace("\","\\")          
    
    #$counter
    #$instance
    #$value
    #$current_tags
    $metric

    
    C:\powershell\send_metrics.ps1 -metric $metric -points $value -hostname $(hostname.exe) -tags $current_tags
}
