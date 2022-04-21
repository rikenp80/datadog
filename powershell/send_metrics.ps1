param
(
$metric, #name of the metric
$points,
$hostname,
$tags
)
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
`"tags`":[`""+$tags+"`"]}
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
$http_request.responseText
