$ErrorActionPreference = "Stop"
$images = @(
    @{ tag = "log-generator:v3"; dir = "services/log_generator" },
    @{ tag = "bronze-worker:v5"; dir = "services/bronze_worker" },
    @{ tag = "my-airflow:v2";    dir = "services/airflow" },
    @{ tag = "my-agent-api:v5";  dir = "services/agent_api" }
)
foreach ($i in $images) {
    docker build -t $i.tag $i.dir
    kind load docker-image $i.tag --name my-cluster
}
"BUILD_DONE"

