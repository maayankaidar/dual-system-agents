import json
import os
import psycopg2
import threading
import uuid
from datetime import datetime, timezone
from typing import Literal

from fastapi import FastAPI
from fastembed import TextEmbedding
from kafka import KafkaConsumer
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException
from ollama import Client
from pydantic import BaseModel
from qdrant_client import QdrantClient

SYSTEM_1_MODEL = "qwen2.5:0.5b"
SYSTEM_2_MODEL = "qwen2.5:7b"

app = FastAPI()
llm = Client(host=os.getenv("OLLAMA_HOST", "http://host.docker.internal:11434"))
qdrant = QdrantClient(url="http://qdrant.lakehouse.svc.cluster.local:6333")
embedder = TextEmbedding(model_name="BAAI/bge-small-en-v1.5")
config.load_incluster_config()
k8s = client.AppsV1Api()

# IN-MEMORY STORE for approvals (YAGNI)
pending_approvals = {}


class Question(BaseModel):
    question: str


class Incident(BaseModel):
    incident_description: str
    target_namespace: str = "streaming"


class Action(BaseModel):
    action: Literal["restart_pod", "no_action"]
    target_service: str
    reason: str
    incident_description: str = ""
    agent: str = ""


def determine_action(incident_description: str, model: str) -> Action:
    prompt = f"Decide a Kubernetes action for this incident. The deployment name is the service mentioned.\nIncident: {incident_description}"
    reply = llm.chat(
        model,
        messages=[{"role": "user", "content": prompt}],
        format=Action.model_json_schema(),
        options={"temperature": 0},
    )
    decision = Action.model_validate_json(reply.message.content)
    decision.incident_description = incident_description
    decision.agent = "System 1 Watchdog (0.5B)" if model == SYSTEM_1_MODEL else "System 2 Chat (7B)"
    return decision


def execute_sql(query: str) -> str:
    """Executes a SQL query on the analytical_db database. 
    Table: system_events (timestamp timestamptz, service text, level text, status_code bigint, response_time_ms bigint, client_ip text, message text).
    Use this for aggregations, counts, and structured analytics."""
    try:
        db_uri = os.getenv("POSTGRES_URI", "postgresql://agent:agentpassword@postgres.lakehouse.svc.cluster.local:5432/analytical_db")
        conn = psycopg2.connect(db_uri)
        cur = conn.cursor()
        cur.execute(query)
        if query.strip().upper().startswith("SELECT"):
            rows = cur.fetchall()
            return str(rows)
        conn.commit()
        return "Executed successfully."
    except Exception as e:
        return f"Error: {e}"
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def search_logs(query: str) -> str:
    """Search unstructured system logs using vector search. Use this for semantic queries or when looking for specific error messages/contexts."""
    vector = next(iter(embedder.embed([query]))).tolist()
    hits = qdrant.query_points("system_logs", query=vector, limit=5).points
    logs = [h.payload["message"] for h in hits]
    return "\n".join(logs)

@app.post("/agent/query")
def query(req: Question):
    messages = [
        {"role": "system", "content": "You are a helpful system agent. You have tools to search logs and execute SQL queries."},
        {"role": "user", "content": req.question}
    ]
    
    response = llm.chat(
        model=SYSTEM_2_MODEL,
        messages=messages,
        tools=[search_logs, execute_sql]
    )
    
    if response.message.tool_calls:
        for tool in response.message.tool_calls:
            if tool.function.name == "search_logs":
                res = search_logs(**tool.function.arguments)
            elif tool.function.name == "execute_sql":
                res = execute_sql(**tool.function.arguments)
            else:
                res = "Unknown tool."
            
            messages.append(response.message)
            messages.append({"role": "tool", "content": str(res), "name": tool.function.name})
            
        response = llm.chat(model=SYSTEM_2_MODEL, messages=messages)

    return {"answer": response.message.content, "context_used": [], "agent": "System 2 Chat (7B)"}


@app.post("/agent/remediate")
def remediate(req: Incident):
    decision = determine_action(req.incident_description, SYSTEM_2_MODEL)
    if decision.action == "restart_pod":
        uid = str(uuid.uuid4())
        pending_approvals[uid] = decision
        return {"status": "pending_approval", "uid": uid, "decision": decision.model_dump()}
    return {"status": "no_action", "decision": decision.model_dump()}


@app.get("/agent/pending")
def get_pending():
    return pending_approvals


@app.post("/agent/approve/{uid}")
def approve(uid: str):
    if uid not in pending_approvals:
        return {"error": "Not found"}
    decision = pending_approvals.pop(uid)
    restarted_at = datetime.now(timezone.utc).isoformat()
    patch = {"spec": {"template": {"metadata": {"annotations": {"kubectl.kubernetes.io/restartedAt": restarted_at}}}}}
    
    try:
        k8s.patch_namespaced_deployment(decision.target_service, "streaming", patch)
        return {"status": "executed", "service": decision.target_service}
    except ApiException as e:
        if e.status == 404:
            return {"error": f"Deployment {decision.target_service} not found in cluster."}
        return {"error": str(e)}


@app.post("/agent/reject/{uid}")
def reject(uid: str):
    if uid in pending_approvals:
        pending_approvals.pop(uid)
    return {"status": "rejected"}


def watchdog_task():
    try:
        consumer = KafkaConsumer(
            "system-logs",
            bootstrap_servers="kafka.streaming.svc.cluster.local:9092",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="watchdog-agent-group"
        )
        for msg in consumer:
            log = msg.value
            if log.get("level") == "ERROR":
                # Watchdog sees an error and autonomously analyzes it using System 1
                incident = f"Service {log.get('service')} threw an error: {log.get('message')}"
                decision = determine_action(incident, SYSTEM_1_MODEL)
                
                if decision.action == "restart_pod":
                    uid = str(uuid.uuid4())
                    pending_approvals[uid] = decision
    except Exception as e:
        print(f"Watchdog error: {e}")


@app.on_event("startup")
def startup_event():
    threading.Thread(target=watchdog_task, daemon=True).start()
