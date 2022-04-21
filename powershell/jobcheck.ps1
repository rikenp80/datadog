<#
Send custom metrics to Datadog -- Job Status
#>
##################################################################
#Get server name and load Powershell module for SQL Server
##################################################################
param
(
$server_instance = (hostname),
$product,
$dc,
$env = "prod"
)
##################################################################
# Set the tags
##################################################################

$tags="""product:"+$product+""",""dc:"+$dc+""",""role:database"",""app:sqlserver"",""env:"+$env+""""
echo $tags

##################################################################
# load assemblies
##################################################################
[Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.Smo") | Out-Null
[Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.SMOExtended") | Out-Null


##################################################################
# Import the SQLPS module so that the Invoke-SQLCMD command works
##################################################################
if ($PSVersionTable.PSVersion.Major -le 2)
{if ( (Get-PSSnapin -Name SqlServerCmdletSnapin100 -ErrorAction SilentlyContinue) -eq $null )
    {Add-PsSnapin SqlServerCmdletSnapin100}

if ( (Get-PSSnapin -Name SqlServerProviderSnapin100 -ErrorAction SilentlyContinue) -eq $null )
    {Add-PsSnapin SqlServerProviderSnapin100}
    }
else
{
##################################################################
# Import the SQLPS module so that the Invoke-SQLCMD command works
##################################################################
    #Import-Module "sqlps"
}

##################################################################
#connect to sql server
##################################################################
        $Server_SQL = New-Object Microsoft.SqlServer.Management.Smo.Server ($server_instance)
        $Server_SQL.ConnectionContext.StatementTimeout = 0
##################################################################
#Function sendMetrics to use to send custom metrics to Datadog
##################################################################
function sendMetrics(
$metric, #name of the metric
$points,
$hostname,
$tags
)
{
echo $metric
echo $points
echo $hostname
echo $tags
$api_key = "dd605d1d620eb2fb227812efe05cb44d"
$app_key = "0a56cc8ba33ed09e8f0f92a8738f40ded31a3fcf"
$http_method = "POST"

$url_signature = "api/v1/series"

$currenttime = (Get-Date -date ((get-date).ToUniversalTime()) -UFormat %s) -Replace("[,\.]\d*", "")

$parameters = "{ `"series`" :
[{`"metric`":`""+$metric+"`",
`"points`":[[$currenttime, "+$points+"]],
`"host`":`""+$hostname+"`",
`"tags`":["+$tags+"]}
]
}"


$url_base = "https://app.datadoghq.com/"
$url = $url_base + $url_signature + "?api_key=$api_key" + "&" + "application_key=$app_key"

$http_request = New-Object -ComObject Msxml2.XMLHTTP
$http_request.open($http_method, $url, $false)
$http_request.setRequestHeader("Content-type", "application/json")

$http_request.setRequestHeader("Connection", "close")
$http_request.send($parameters)
$http_request.status
$http_request.responseText}

##################################################################
#In order to keep track of the records sent to Datadog
#The records are kept on a table named JobStatusHistory on 
#DbManagement. This is to avoid sending duplicate data to Datadog
##################################################################
#Check if there are pending records to insert
##################################################################
$query="Declare @a int = (SELECT CONVERT(INT,CONVERT(char(4),YEAR(getdate()))+  `
Case when LEN(RTRIM(CONVERT(char(2),MONTH(getdate())-1)))=1 then  '0'+RTRIM(CONVERT(char(2),MONTH(getdate())-1))  `
ELSE  RTRIM(CONVERT(char(2),MONTH(getdate())-1))  END  + CASE WHEN LEN(CONVERT(char(2),DATEPART(d,getdate())))=1 then  `
'0'+CONVERT(char(2),DATEPART(d,getdate()))  ELSE  CONVERT(char(2),DATEPART(d,getdate()))  END));  `
If exists (select sjh.instance_id, sj.name, sj.description,  sjh.job_id, sjh.step_id, sjh.step_name, sjh.message,  `
sjh.sql_message_id,sjh.run_duration,sjh.run_time,  sjh.run_status,  Case  When sjh.run_status=0 then 'Failed'  `
When sjh.run_status=1 then 'Succesful'  When sjh.run_status=3 then 'Cancelled' `
 When sjh.run_status=4 then 'In Progress'  END as run_status_descrip,  sjh.run_date,  `
 sjh.server,  cat.name as category,  cat.category_id  from msdb..sysjobhistory sjh  `
 Inner join msdb..sysjobs sj  on sjh.job_id=sj.job_id  Left JOIN msdb..syscategories cat  `
 on sj.category_id=cat.category_id  Where sjh.run_date>=@a and sjh.instance_id not in `
 (SELECT instance_id from dbManagement..JobStatusHistory))  Select 'Y' as [Decision]  Else  Select 'N' as [Decision]"
$Decision=Invoke-Sqlcmd -ServerInstance $server_instance -database "msdb" -Query $query
echo $Decision

##################################################################
#Insert Into Table on dbManagament if there are unwritten
#records from msdb..sysjobhistory table
##################################################################
If($Decision.Decision -eq "Y"){$query="Declare @a int = (SELECT CONVERT(INT,CONVERT(char(4),YEAR(getdate()))+ `
Case when LEN(RTRIM(CONVERT(char(2),MONTH(getdate())-1)))=1 then `
'0'+RTRIM(CONVERT(char(2),MONTH(getdate())-1)) `
ELSE `
RTRIM(CONVERT(char(2),MONTH(getdate())-1)) `
END `
+ CASE WHEN LEN(CONVERT(char(2),DATEPART(d,getdate())))=1 then `
'0'+CONVERT(char(2),DATEPART(d,getdate())) `
ELSE `
CONVERT(char(2),DATEPART(d,getdate())) `
END)) `
Insert into [dbo].[JobStatusHistory] `
(instance_id,[jobname],[description],[job_id], `
[step_id],[step_name],[message],[sql_message_id], `
[run_duration],[run_time],[run_status],[run_status_descrip], `
[run_date],[server],[category],[category_id]) `
(select sjh.instance_id, sj.name, sj.description, `
sjh.job_id, sjh.step_id, sjh.step_name, sjh.message, `
sjh.sql_message_id,sjh.run_duration,sjh.run_time, `
sjh.run_status, `
Case `
When sjh.run_status=0 then 'Failed' `
When sjh.run_status=1 then 'Succesful' `
When sjh.run_status=3 then 'Cancelled' `
When sjh.run_status=4 then 'In Progress' `
END as run_status_descrip, `
sjh.run_date, `
sjh.server, `
cat.name as category, `
cat.category_id `
from msdb..sysjobhistory sjh `
Inner join msdb..sysjobs sj `
on sjh.job_id=sj.job_id `
Left JOIN msdb..syscategories cat `
on sj.category_id=cat.category_id `
Where sjh.run_date>=@a and sjh.instance_id not in (SELECT instance_id from dbManagement..JobStatusHistory))"

Invoke-Sqlcmd -ServerInstance $server_instance -database "dbManagement" -Query $query

}

##################################################################
#Check if there are pending records to send to Datadog
#Records with sent = 0 are pending to be sent to Datadog
##################################################################
$query="If exists (SELECT [instance_id]
      ,[jobname]
      ,[description]
      ,[job_id]
      ,[step_id]
      ,[step_name]
      ,[message]
      ,[sql_message_id]
      ,[run_duration]
      ,[run_time]
      ,[run_status]
      ,[run_status_descrip]
      ,[run_date]
      ,[server]
      ,[category]
      ,[category_id]
  FROM [dbManagement].[dbo].[JobStatusHistoryPending])
Select 'Y' as [Decision]
Else
Select 'N' as [Decision]"
$Decision=Invoke-Sqlcmd -ServerInstance $server_instance -database "dbManagement" -Query $query
echo $Decision

##################################################################
#Get Data to be sent to Datadog if there is any pending records
##################################################################
If ($Decision.Decision -eq "Y")
{
$query="SELECT [instance_id]
      ,[jobname]
      ,[description]
      ,[job_id]
      ,[step_id]
      ,[step_name]
      ,[message]
      ,[sql_message_id]
      ,[run_duration]
      ,[run_time]
      ,[run_status]
      ,[run_status_descrip]
      ,[run_date]
      ,[server]
      ,[category]
      ,[category_id]
  FROM [dbManagement].[dbo].[JobStatusHistoryPending]
  GO"
         $jobStats=Invoke-Sqlcmd -ServerInstance $server_instance -database "dbManagement" -Query $query
         
##################################################################
#Sending records one at a time
##################################################################
foreach($jobStat in $jobStats)
{

$current_tags=$tags+",""sqlserver:" + $jobStat.server `
 + """,""jobName:" + $jobStat.jobname + `
""",""jobDescrip:" + $jobStat.description + """,""jobId:" + $jobStat.job_id + """,""step_id:" + $jobStat.step_id + """,""step_name:" + $jobStat.step_name `
+ """,""sqlmessageid:" + $jobStat.sql_message_id + """,""run_duration:" + $jobStat.run_duration + """,""run_time:" + $jobStat.run_time + """,""run_status:" + $jobStat.run_status + `
""",""run_status_descrip:" + $jobStat.run_status_descrip + """,""run_date:" + $jobStat.run_date + """,""jobcategory:" + $jobStat.category + """,""categoryid:" + $jobStat.category_id + """"
$current_tags=$current_tags.Replace("\","\\")
sendMetrics -metric sqlserver.job.status -points $jobStat.run_status -hostname $(hostname.exe) -tags $current_tags

}
##################################################################
#Set sent column to 1 for the records we sent to Datadog
##################################################################
$query="If exists (SELECT * from JobStatusHistoryPending)
UPDATE JobStatusHistoryPending
SET [sent]=1
GO"

 
 Invoke-Sqlcmd -ServerInstance $server_instance -database "dbManagement" -Query $query
 }
##################################################################         
#Cleanup table JobStatusHistory to preserve only one month of data
##################################################################
 $query="Declare @a int = (SELECT CONVERT(INT,CONVERT(char(4),YEAR(getdate()))+
Case when LEN(RTRIM(CONVERT(char(2),MONTH(getdate())-1)))=1 then
'0'+RTRIM(CONVERT(char(2),MONTH(getdate())-1))
ELSE
RTRIM(CONVERT(char(2),MONTH(getdate())-1))
END
+ CASE WHEN LEN(CONVERT(char(2),DATEPART(d,getdate())))=1 then
'0'+CONVERT(char(2),DATEPART(d,getdate()))
ELSE
CONVERT(char(2),DATEPART(d,getdate()))
END))
 Delete [dbo].[JobStatusHistory]
where run_date <@a"

Invoke-Sqlcmd -ServerInstance $server_instance -database "dbManagement" -Query $query
