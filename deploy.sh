#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="my-cluster"

echo "=== Dual-System Agents Cluster Deployment ==="

# 1. Verify Prerequisites
if ! command -v docker &> /dev/null; then
    echo "ERROR: docker CLI is not installed or not in PATH."
    exit 1
fi

# Ensure docker access safely via group session without compromising socket permissions
if ! docker ps > /dev/null 2>&1; then
    if [ "${IN_SG_DOCKER:-0}" = "0" ]; then
        echo "Activating docker group permissions for this execution..."
        export IN_SG_DOCKER=1
        exec sg docker -c "$0 $*"
    else
        echo "ERROR: Cannot connect to docker daemon. Ensure docker service is running."
        exit 1
    fi
fi

if ! command -v kind &> /dev/null; then
    echo "ERROR: kind CLI is not installed or not in PATH."
    exit 1
fi

if ! command -v kubectl &> /dev/null; then
    echo "ERROR: kubectl CLI is not installed or not in PATH."
    exit 1
fi

if [ ! -f .env ]; then
    echo "ERROR: .env file not found. Please create .env with GEMINI_API_KEY=your_key."
    exit 1
fi

# 2. Create or verify Kind Cluster
if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    echo "Kind cluster '${CLUSTER_NAME}' is already running."
else
    echo "Creating lightweight single-node Kind cluster '${CLUSTER_NAME}'..."
    kind create cluster --name "${CLUSTER_NAME}" --config infra/kind-config.yaml
fi

# 3. Create Namespaces
echo "Applying namespaces..."
kubectl apply -f k8s/01-namespace.yaml
kubectl apply -f k8s/04-lakehouse.yaml

# 4. Create/Update Gemini Secret securely from .env
echo "Configuring Gemini API Secret..."
kubectl create secret generic gemini-secret \
    --from-env-file=.env \
    -n lakehouse \
    --dry-run=client -o yaml | kubectl apply -f -

# 5. Build Docker Images
echo "Building local container images..."
images=(
    "log-generator:latest services/log_generator"
    "bronze-worker:latest services/bronze_worker"
    "my-airflow:latest services/airflow"
    "my-agent-api:latest services/agent_api"
    "my-chat-ui:latest ui"
)

for item in "${images[@]}"; do
    tag=$(echo "$item" | awk '{print $1}')
    dir=$(echo "$item" | awk '{print $2}')
    echo "--> Building ${tag} from ${dir}..."
    docker build -t "${tag}" "${dir}"
    echo "--> Loading ${tag} into Kind cluster..."
    kind load docker-image "${tag}" --name "${CLUSTER_NAME}"
done

# 6. Apply all Kubernetes manifests
echo "Deploying Kubernetes manifests..."
kubectl apply -f k8s/02-kafka.yaml
kubectl apply -f k8s/06-postgres.yaml
kubectl apply -f k8s/08-qdrant.yaml
kubectl apply -f k8s/03-log-generator.yaml
kubectl apply -f k8s/05-bronze-worker.yaml
kubectl apply -f k8s/07-airflow.yaml
kubectl apply -f k8s/09-agent-api.yaml
kubectl apply -f k8s/10-chat-ui.yaml

echo ""
echo "=== Deployment Submitted Successfully! ==="
echo "Check pod status with:"
echo "  kubectl get pods -A"
echo ""
echo "To access the services locally, run port-forwards:"
echo "  # Streamlit Chat & HITL UI (http://localhost:8501):"
echo "  kubectl port-forward svc/chat-ui -n lakehouse 8501:8501"
echo ""
echo "  # Agent API (http://localhost:8000/docs):"
echo "  kubectl port-forward svc/agent-api -n lakehouse 8000:8000"
echo ""
echo "  # Airflow Web UI (http://localhost:8080):"
echo "  kubectl port-forward svc/airflow-webserver -n lakehouse 8080:8080"
echo ""
echo "  # MinIO Console (http://localhost:9001):"
echo "  kubectl port-forward svc/minio -n lakehouse 9001:9001"
