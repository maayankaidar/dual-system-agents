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

*   **System 1 (Watchdog):** A fast, low-latency agent (`gemini-flash-lite-latest`) continuously monitoring Kafka event streams (e.g., system logs). It identifies anomalies and proposes remediation actions, adding them to a Human-In-The-Loop (HITL) approval queue.
*   **System 2 (Chat/RAG):** An analytical agent (`gemini-3.8-flash`) handling the interactive Streamlit UI. It answers complex user queries using Retrieval-Augmented Generation (RAG) against system logs and SQL analytics, and executes remediation actions when instructed or when HITL approvals are granted.

## Features

*   **Kafka Event Streaming:** Simulates a real-time log stream using KRaft mode.
*   **Vector Database (Qdrant):** Embeds and stores logs for semantic RAG search.
*   **Analytical Storage (PostgreSQL & MinIO):** Structured analytical database and raw Parquet lakehouse storage with Airflow ETL.
*   **Human-In-The-Loop (HITL):** Proposed actions by System 1 require user approval before execution.
*   **Kubernetes (Kind):** Microservices orchestrated across `streaming` and `lakehouse` namespaces.
*   **Gemini API Models:** Fast, intelligent reasoning with zero local model memory footprint.

## Components

1.  `kafka-producer`: Generates mock system logs and anomalies.
2.  `qdrant`: Vector database for RAG.
3.  `agent-api`: FastAPI backend hosting the Dual-System agents.
    *   Watchdog task (System 1) consuming Kafka.
    *   REST endpoints (System 2) for chat and approvals.
4.  `chat-ui`: Streamlit frontend for interacting with the system.

## Setup Instructions

1.  **Prerequisites:** Docker, Kind, `kubectl`.
2.  **Configure API Key:**
    Create a `.env` file in the project root:
    ```bash
    echo "GEMINI_API_KEY=your_gemini_api_key" > .env
    ```
3.  **Deploy Cluster:**
    ```bash
    ./deploy.sh
    ```
4.  **Access Services:**
    - Streamlit Chat & HITL UI: `kubectl port-forward svc/chat-ui -n lakehouse 8501:8501` -> [http://localhost:8501](http://localhost:8501)
    - Agent API: `kubectl port-forward svc/agent-api -n lakehouse 8000:8000` -> [http://localhost:8000/docs](http://localhost:8000/docs)
    - Airflow Webserver: `kubectl port-forward svc/airflow-webserver -n lakehouse 8080:8080` -> [http://localhost:8080](http://localhost:8080)
5.  **Teardown:**
    ```bash
    ./teardown.sh
    ```

