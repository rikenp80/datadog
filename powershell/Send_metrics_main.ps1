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


#Get publication info
         $query="exec sp_replmonitorhelppublication @publisher='" + $server_instance +"'"

         $pubs=(Invoke-Sqlcmd -ServerInstance $server_instance -database "distribution" -Query $query)
         
foreach($pub in $pubs)
{
#Build query to insert rows in the publication table
$query="Insert into [dbo].[repl_publication]
(publisher_db,
publication,
publication_id,
publication_type,
status,
warning,
worst_latency,
best_latency,
average_latency,
last_distsync,
retention,
latencythreshold,
expirationthreshold,
agentnotrunningthreshold,
subscriptioncount,
runningdistagentcount,
snapshot_agentname,
logreader_agentname,
qreader_agentname,
worst_runspeedPerf,
best_runspeedPerf,
average_runspeedPerf,
retention_period_unit,
publisher,
CreatedDate)
values('"+$pub.publisher_db+"', '"`
+$pub.publication +"', "`
+$pub.publication_id+", "`
+$pub.publication_type+", "`
+$pub.status+", "`
+$pub.warning+", "`
+$pub.worst_latency+", "`
+$pub.best_latency+", "`
+$pub.average_latency+", '"`
+$pub.last_distsync+"', "`
+$pub.retention+", "`
+$pub.latencythreshold+", "`
+$pub.expirationthreshold+", "`
+$pub.agentnotrunningthreshold+", "`
+$pub.subscriptioncount+", "`
+$pub.runningdistagentcount+", '"`
+$pub.snapshot_agentname+"', '"`
+$pub.logreader_agentname+"', '"`
+$pub.qreader_agentname+"', "`
+$pub.worst_runspeedPerf+", "`
+$pub.best_runspeedPerf+", "`
+$pub.average_runspeedPerf+", "`
+$pub.retention_period_unit+", '"`
+$server_instance+"', "`
+"sysdatetime())"
#Replace empty strings with NULL
   $query=$query.replace(" ,"," NULL,")
   $query=$query.replace(" '',"," NULL,")
#Execute query to insert record on publication table   
   (Invoke-Sqlcmd -ServerInstance $server_instance -database "dbManagement" -Query $query)   
 #Send custom metrics to datadog
#$com="Set-ExecutionPolicy -Scope Process -ExecutionPolicy ByPass;C:\powershell\send_metrics.ps1 -metric `"sqlserver.repl.pub.status`" -points "`
#+$pub.status+" -hostname `""+$server_instance+"`" -tags `"database:"+$server_instance+"."+$pub.publisher_db+"`";"
#& $com
         function sendMetrics($metric, $points)
         {
         $tags="database:"+$server_instance+"."+$pub.publisher_db
         C:\powershell\send_metrics.ps1 -metric $metric -points $points -hostname "$server_instance" -tags "$tags"
         echo $tags
         }
         sendMetrics -metric "sqlserver.repl.pub.status" -points $pub.status
          if ($pub.status -ge 4)
          {sendMetrics -metric "sqlserver.repl.pub.failed" -points 1}
          else
          {sendMetrics -metric "sqlserver.repl.pub.failed" -points 0}
}

#Get data from subscriptions
        #Build Query to get data on subscriptions
        $query="EXEC sp_replmonitorhelpsubscription @publisher='"+$server_instance+"', @publication_type=0"
        $subs=(Invoke-Sqlcmd -ServerInstance $server_instance -database "distribution" -Query $query)
        foreach($sub in $subs)
        {
        #Build query to get data on pending commands to be replicated
        $query="exec sp_replmonitorsubscriptionpendingcmds @publisher='"+ $server_instance`
        +"', @publisher_db='"+$sub.publisher_db+"', @publication='"`
        +$sub.publication+"', @subscriber='"+$sub.subscriber+"', @subscriber_db='"`
        +$sub.subscriber_db+"',  @subscription_type="+$sub.subtype
        #Execute query
        $pendcmds=(Invoke-Sqlcmd -ServerInstance $server_instance -database "distribution" -Query $query)
        #Build query to insert subscription data into a table
        $query="Insert into [dbo].[repl_subscription]
(status,
	warning,
	subscriber,
	subscriber_db,
	publisher_db,
	publication,
	publication_type,
	subtype,
	latency,
	latencythreshold,
	agentnotrunning,
	agentnotrunningthreshold,
	timetoexpiration,
	expirationthreshold,
	last_distsync,
	distribution_agentname,
	mergeagentname,
	mergesubscriptionfriendlyname,
	mergeagentlocation,
	mergeconnectiontype,
	mergePerformance,
	mergerunspeed,
	mergerunduration,
	monitorranking,
	distributionagentjobid,
	mergeagentjobid,
	distributionagentid,
	distributionagentprofileid,
	mergeagentid,
	mergeagentprofileid,
	logreaderagentname,
	publisher,
    pendingtransactions,
	pendingtransactiontime,
	CreatedDate)
values("+$sub.status+", "`
+$sub.warning+",'"`
+$sub.subscriber+"', '"`
+$sub.subscriber_db+"', '"`
+$sub.publisher_db+"', '"`
+$sub.publication+"', "`
+$sub.publication_type+", "`
+$sub.subtype+", "`
+$sub.latency+", "`
+$sub.latencythreshold+", "`
+$sub.agentnotrunning+", "`
+$sub.agentnotrunningthreshold+", "`
+$sub.timetoexpiration+", "`
+$sub.expirationthreshold+", '"`
+$sub.last_distsync+"', '"`
+$sub.distribution_agentname+"', '"`
+$sub.mergeagentname+"', '"`
+$sub.mergesubscriptionfriendlyname+"', '"`
+$sub.mergeagentlocation+"', "`
+$sub.mergeconnectiontype+", "`
+$sub.mergePerformance+", "`
+$sub.mergerunspeed+", "`
+$sub.mergerunduration+", "`
+$sub.monitorranking+", NULL, NULL, "`
+$sub.distributionagentid+", "`
+$sub.distributionagentprofileid+", "`
+$sub.mergeagentid+", "`
+$sub.mergeagentprofileid+", '"`
+$sub.logreaderagentname+"', '"`
+$server_instance+"', "`
+$pendcmds.pendingcmdcount+", "`
+$pendcmds.estimatedprocesstime+", "`
+"sysdatetime())"
#Replace empty strings with NULL
   $query=$query.replace(" ,"," NULL,")
   $query=$query.replace(" '',"," NULL,")
#Execute Query
   (Invoke-Sqlcmd -ServerInstance $server_instance -database "dbManagement" -Query $query)
       # $com="Set-ExecutionPolicy -Scope Process -ExecutionPolicy ByPass;
        # C:\powershell\send_metrics.ps1 -metric `"sqlserver.repl.sub.status`" -points "`
        # +$sub.status+" -hostname `""+$server_instance+"`" -tags `"database:"+$server_instance+"."+$sub.subscriber_db`
         #+"`";"
         #+"C:\powershell\send_metrics.ps1 -metric `"sqlserver.repl.sub.warning`" -points "`
         #+$sub.warning+" -hostname `""+$server_instance+"`" -tags `"database:"`
         #+$server_instance+"."+$sub.subscriber_db`
         #+"`";C:\powershell\send_metrics.ps1 -metric `"sqlserver.repl.sub.latency`" -points "`
         #+$sub.latency+" -hostname `""+$server_instance`
         #+"`" -tags `"database:"+$server_instance+"."+$sub.subscriber_db+`
         #"`";C:\powershell\send_metrics.ps1 -metric `"sqlserver.repl.sub.pendingtran`" -points "`
         #+$pendcmds.pendingcmdcount+" -hostname `""+$server_instance+`
         #"`" -tags `"database:"+$server_instance+"."+$sub.subscriber_db+`
         #"`";C:\powershell\send_metrics.ps1 -metric `"sqlserver.repl.sub.pendingtrantime`" -points "`
         #+$pendcmds.estimatedprocesstime+" -hostname `""+$server_instance+"`" -tags `"database:"+$server_instance+"."+$sub.subscriber_db + "`"";

        #echo $com
         function sendMetrics($metric, $points)
         {
         $tags="database:"+$server_instance+"."+$sub.publisher_db
         C:\powershell\send_metrics.ps1 -metric $metric -points $points -hostname "$server_instance" -tags "$tags"
         }
         sendMetrics -metric "sqlserver.repl.sub.status" -point $sub.status
         sendMetrics -metric "sqlserver.repl.sub.warning" -point $sub.warning
         sendMetrics -metric "sqlserver.repl.sub.latency" -point $sub.latency
         sendMetrics -metric "sqlserver.repl.sub.pendingtran" -point $pendcmds.pendingcmdcount
         sendMetrics -metric "sqlserver.repl.sub.pendingtrantime" -point $pendcmds.estimatedprocesstime
         sendMetrics -metric "sqlserver.repl.sub.monitorranking" -point $sub.monitorranking
         if ($sub.status -ge 4)
         {sendMetrics -metric "sqlserver.repl.sub.failed" -point 1}
         else
         {sendMetrics -metric "sqlserver.repl.sub.failed" -point 0}
         if ($sub.monitorranking -eq 60)
         {sendMetrics -metric "sqlserver.repl.sub.error" -point 1}
         else
         {sendMetrics -metric "sqlserver.repl.sub.error" -point 0}
         if ($sub.monitorranking -eq 56)
         {sendMetrics -metric "sqlserver.repl.sub.warning.perf" -point 1}
         else
         {sendMetrics -metric "sqlserver.repl.sub.warning.perf" -point 0}
         if ($sub.monitorranking -eq 52)
         {sendMetrics -metric "sqlserver.repl.sub.warning.exp" -point 1}
         else
         {sendMetrics -metric "sqlserver.repl.sub.warning.exp" -point 0}
        }