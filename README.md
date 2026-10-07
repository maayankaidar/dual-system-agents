# Dual-System Agents

A Dual-System cognitive agent architecture for cluster observability: **System 1** provides fast anomaly detection over real-time log streams, while **System 2** handles deep log retrieval, analytical SQL querying, and guided cluster remediation.

## Architecture

```mermaid
flowchart TD
    %% Define Styles
    classDef k8s fill:#326ce5,stroke:#fff,stroke-width:2px,color:#fff;
    classDef host fill:#4285F4,stroke:#fff,stroke-width:2px,color:#fff;
    classDef db fill:#00a300,stroke:#fff,stroke-width:2px,color:#fff;
    classDef ui fill:#ff4b4b,stroke:#fff,stroke-width:2px,color:#fff;
    classDef agent fill:#f4b400,stroke:#fff,stroke-width:2px,color:#fff,font-weight:bold;

    subgraph K8s["Kubernetes Cluster (Kind)"]
        UI["💻 Streamlit Chat UI"]:::ui
        
        subgraph Agents["Dual-System Agent API"]
            AgentAPI["🤖 FastAPI Service<br>(agent-api)"]:::agent
            Sys1["System 1 (Watchdog)<br>Model: Qwen 0.5B<br>Fast, Reactive"]:::agent
            Sys2["System 2 (Chat/RAG)<br>Model: Qwen 7B<br>Slow, Analytical"]:::agent
        end
        
        subgraph Orchestration["ETL Pipelines"]
            BronzeWorker["👷 Bronze Worker<br>(Kafka to MinIO)"]
            Airflow["⚙️ Apache Airflow DAGs<br>(Bronze to Silver)"]
        end

        subgraph Data["Data & Streaming Layer"]
            Producer["📝 Log Generator<br>(Mock/Sample Data)"]
            Kafka["⚡ Kafka / Redpanda<br>(Log Stream)"]:::db
            MinIO["🗄️ MinIO<br>(Raw Parquet Storage)"]:::db
            Postgres["🐘 PostgreSQL<br>(Structured Analytics DB)"]:::db
            Qdrant["🎯 Qdrant<br>(Vector DB for RAG)"]:::db
        end
    end

    subgraph Host["Host PC (Windows)"]
        Ollama["🧠 Ollama Server"]:::host
        Models["📦 Local LLMs"]:::host
    end

    %% Data Flow & Connections
    %% Interactions
    UI <-->|Queries & Approvals| AgentAPI
    AgentAPI <-->|Routes Queries| Sys2
    Sys1 -->|Proposes Action| AgentAPI
    AgentAPI <-->|HTTP Requests| Ollama
    Ollama <-->|Loads/Unloads| Models
    Sys2 <-->|Vector Search| Qdrant
    Sys2 <-->|Executes SQL| Postgres

    %% Pipeline Data Flow
    Producer -->|Produces Logs| Kafka
    Kafka -->|Streams Logs| Sys1
    
    Kafka -->|Streams Logs| BronzeWorker
    BronzeWorker -->|Writes Parquet| MinIO
    
    MinIO -->|Batch Reads| Airflow
    Airflow -->|Writes Structured| Postgres
    Airflow -->|Embeds & Indexes| Qdrant
```

*   **System 1 (Watchdog):** A small, fast, low-latency agent (`qwen2.5:0.5b`) continuously monitoring Kafka event streams (e.g., system logs). It identifies anomalies and proposes remediation actions, adding them to a Human-In-The-Loop (HITL) approval queue.
*   **System 2 (Chat/RAG):** A larger, more capable agent (`qwen2.5:7b`) handling the interactive Streamlit UI. It answers complex user queries using Retrieval-Augmented Generation (RAG) against system logs and executes remediation actions when instructed or when HITL approvals are granted.

## Features

*   **Kafka Event Streaming:** Simulates a real-time log stream.
*   **Vector Database (Qdrant):** Embeds and stores logs for RAG.
*   **Human-In-The-Loop (HITL):** Proposed actions by System 1 require user approval before execution.
*   **Kubernetes (Kind):** The entire backend runs in a local Kubernetes cluster.
*   **Local LLMs (Ollama):** Powered entirely by locally hosted models, ensuring privacy and zero API costs. 
    * *Note on Architecture:* In this local setup, Ollama runs on the Host PC rather than inside the Kubernetes cluster. This is an intentional design choice for local development, as passing Windows Host GPUs into local Docker/Kubernetes containers is notoriously complex. In a production environment, the inference engine would be deployed directly onto GPU-enabled Kubernetes nodes.

## Components

1.  `kafka-producer`: Generates mock system logs and anomalies.
2.  `qdrant`: Vector database for RAG.
3.  `agent-api`: FastAPI backend hosting the Dual-System agents.
    *   Watchdog task (System 1) consuming Kafka.
    *   REST endpoints (System 2) for chat and approvals.
4.  `chat-ui`: Streamlit frontend for interacting with the system.

## Setup Instructions

1.  **Prerequisites:** Docker, Kind, `kubectl`, Ollama.
2.  **Pull Models:**
    ```bash
    ollama pull qwen2.5:7b
    ollama pull qwen2.5:0.5b
    ollama pull nomic-embed-text
    ```
3.  **Start Cluster:**
    (Follow standard Kind cluster creation and deployment steps as defined in your workflow)

