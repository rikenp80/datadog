$counters = `
'
\SQLServer:Databases(*)\Write Transactions/sec,
\SQLServer:Databases(*)\Transactions/sec,
\SQLServer:SQL Statistics\Batch Requests/sec,
\SQLServer:General Statistics\Processes blocked,
\PhysicalDisk(*)\Avg. Disk sec/Read,
\PhysicalDisk(*)\Avg. Disk sec/Write,
\SQLServer:Access Methods\Page Splits/sec,
\SQLServer:Wait Statistics(*)\Log write waits,
\SQLServer:Wait Statistics(*)\Page IO latch waits,
\PhysicalDisk(*)\Current Disk Queue Length,
\PhysicalDisk(*)\Avg. Disk Read Queue Length,
\PhysicalDisk(*)\Avg. Disk Write Queue Length,
\PhysicalDisk(*)\Disk Read Bytes/sec,
\PhysicalDisk(*)\Disk Write Bytes/sec,
\PhysicalDisk(*)\Disk Bytes/sec,
\Processor Information(*)\% Processor Time,
\LogicalDisk(*)\% Free Space,
\LogicalDisk(*)\Free Megabytes,
\SQLServer:Replication Logreader(*)\Logreader:Delivered Cmds/sec,
\SQLServer:Replication Dist.(*)\Dist:Delivery Latency
'

while ($true)
{
    C:\Powershell\send_metrics_Perfmon.ps1 -metric_group 'sqlserver' -sample_interval 1 -max_samples 60 -counters $counters
}