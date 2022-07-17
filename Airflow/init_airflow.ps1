Write-Host "* Airflow Initialization Script for Wizeline's DEB *" -ForegroundColor DarkCyan

$voice = New-Object -ComObject Sapi.spvoice
$voice.rate = 0
$voice.volume = 100
$voiceOutput = $voice.speak("Airflow Initialization Script for Wizeline's Data Engineer Bootcamp, has started", 1)

Write-Host '** Updating instance name **' -ForegroundColor DarkYellow
$fileContent            = (Get-Content .\terraform.tfvars)
$matcher                = Select-String -InputObject $fileContent -Pattern 'instance_name     = "data-bootcamp-(\d+)"'
$version                = $matcher.Matches.Groups[1].Value -as [int32]
$versionUpdated         = $version + 1
$instanceNameUpdated    = 'data-bootcamp-' + $versionUpdated
$fileContentUpdated     = $fileContent -replace 'data-bootcamp-\d+', $instanceNameUpdated
$fileContentUpdated | Out-File -FilePath '.\terraform.tfvars' -Encoding ASCII
Write-Host ('** Instance Name updated from "data-bootcamp-{0}" to "data-bootcamp-{1}" **' -f $version, $versionUpdated)-ForegroundColor DarkYellow

terraform init
terraform init --upgrade
$voiceOutput = $voice.speak("Creating infrastructure, please confirm terraform plan", 1)
terraform apply --var-file=terraform.tfvars

$voiceOutput = $voice.speak("Infrastucture creation complete, Installing Airflow", 1)
gcloud container clusters get-credentials $(terraform output -raw kubernetes_cluster_name) --region $(terraform output -raw location)

kubectl create namespace nfs
kubectl -n nfs apply -f nfs/nfs-server.yaml
Set-Variable  NFS_SERVER $(kubectl -n nfs get service/nfs-server -o jsonpath="{.spec.clusterIP}")

kubectl create namespace storage
helm repo add nfs-subdir-external-provisioner https://kubernetes-sigs.github.io/nfs-subdir-external-provisioner/
helm install nfs-subdir-external-provisioner nfs-subdir-external-provisioner/nfs-subdir-external-provisioner `
--namespace storage `
--set nfs.server=$NFS_SERVER `
--set nfs.path=/

kubectl create namespace airflow
helm repo add apache-airflow https://airflow.apache.org
helm upgrade --install airflow -f airflow-values.yaml apache-airflow/airflow --namespace airflow

Write-Host '** Airflow should have been installed successfully, statrting server **' -ForegroundColor DarkYellow
$voiceOutput = $voice.speak("The script has finished!", 1)
kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow

