import streamlit as st
import requests
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

AGENT_API_URL = os.getenv("AGENT_API_URL", "http://localhost:8000")

st.set_page_config(page_title="Dual-System Agents", page_icon="🤖", layout="wide")

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = [{"role": "assistant", "content": "Hi! I'm monitoring the cluster. How can I help?"}]

if "action_history" not in st.session_state:
    st.session_state.action_history = []

if "chat_executor" not in st.session_state:
    st.session_state.chat_executor = ThreadPoolExecutor(max_workers=2)

if "pending_chat_future" not in st.session_state:
    st.session_state.pending_chat_future = None


def _execute_chat_query(prompt_text: str, is_action_req: bool):
    """Runs in a background thread so sidebar Approve/Reject clicks never block or abort chat queries."""
    try:
        if is_action_req:
            res = requests.post(
                f"{AGENT_API_URL}/agent/remediate",
                json={"incident_description": prompt_text, "target_namespace": "streaming"},
                timeout=60,
            )
            if res.status_code == 200:
                resp = res.json()
                decision = resp.get("decision", {})
                agent_name = decision.get("agent", "System 2 Remediation Agent")
                if resp.get("status") == "pending_approval":
                    answer = (
                        f"**Agent:** {agent_name}\n\n"
                        f"**Recommendation:** `{decision.get('action')}` on `{decision.get('target_service')}`\n\n"
                        f"**Reasoning:** {decision.get('reason')}\n\n"
                        f"👉 *Action placed into the **Watchdog Approvals** sidebar for Human-in-the-Loop review.*"
                    )
                else:
                    answer = (
                        f"**Agent:** {agent_name}\n\n"
                        f"**Action Taken:** `{decision.get('action')}` on `{decision.get('target_service')}`\n\n"
                        f"**Reasoning:** {decision.get('reason')}"
                    )
                return ("ok", answer)
            return ("error", f"Backend Error: {res.text}")
        else:
            res = requests.post(
                f"{AGENT_API_URL}/agent/query",
                json={"question": prompt_text},
                timeout=120,
            )
            if res.status_code == 200:
                data = res.json()
                agent_name = data.get("agent", "System 2 Chat")
                answer_text = data.get("answer", "No answer found.")
                return ("ok", f"**Agent:** {agent_name}\n\n{answer_text}")
            return ("error", f"Backend Error: {res.text}")
    except requests.exceptions.Timeout:
        return ("error", "⏳ Request timed out while waiting for agent analysis. Please try again.")
    except requests.exceptions.ConnectionError:
        return ("error", "⚠️ Cannot connect to the Agent API on localhost:8000.")
    except Exception as e:
        return ("error", f"⚠️ Error: {e}")

# Clean up any leftover HITL audit messages from previous sessions to keep chat pristine
st.session_state.messages = [
    m for m in st.session_state.messages
    if "HITL Remediation" not in m.get("content", "") and "HITL Action" not in m.get("content", "")
]

# --- SIDEBAR: Watchdog Approvals & Action History ---
with st.sidebar:
    st.header("🛡️ Watchdog Approvals")
    st.caption("⚡ Live Human-in-the-Loop queue (auto-syncs every 4s)")

    @st.fragment(run_every=4)
    def render_watchdog_queue():
        col_hdr, col_btn = st.columns([2, 1])
        with col_hdr:
            st.write("**Pending Actions:**")
        with col_btn:
            if st.button("🔄 Sync", key="sync_queue_btn", help="Force immediate queue check"):
                pass

        try:
            res = requests.get(f"{AGENT_API_URL}/agent/pending", timeout=3)
            if res.status_code == 200:
                pending = res.json()
                if not pending:
                    st.success("🟢 No pending incidents. All streams nominal.")
                else:
                    for uid, decision in list(pending.items()):
                        target_svc = decision.get("target_service", "service")
                        with st.expander(f"⚠️ Action: Restart `{target_svc}`", expanded=True):
                            st.markdown(f"**Agent:** {decision.get('agent', 'System 1 Watchdog (Flash Lite)')}")
                            st.markdown(f"**Reason:** {decision.get('reason')}")
                            st.caption(f"**Trigger:** `{decision.get('incident_description')}`")

                            c1, c2 = st.columns(2)
                            with c1:
                                if st.button("✅ Approve", key=f"approve_{uid}", use_container_width=True):
                                    with st.spinner(f"Applying patch for `{target_svc}`..."):
                                        app_res = requests.post(f"{AGENT_API_URL}/agent/approve/{uid}")
                                        if app_res.status_code == 200 and "error" not in app_res.json():
                                            resp_data = app_res.json()
                                            details = resp_data.get("details", f"Rollout restart triggered on `{target_svc}`")
                                            st.toast(f"✅ Approved: {details}", icon="🚀")
                                            st.session_state.action_history.insert(0, {
                                                "time": datetime.now().strftime("%H:%M:%S"),
                                                "status": "approved",
                                                "service": target_svc,
                                                "details": details,
                                                "reason": decision.get("reason", "")
                                            })
                                        else:
                                            err = app_res.json().get("error", "Failed to apply remediation")
                                            st.error(err)
                                    time.sleep(0.3)
                                    st.rerun()

                            with c2:
                                if st.button("❌ Reject", key=f"reject_{uid}", use_container_width=True):
                                    requests.post(f"{AGENT_API_URL}/agent/reject/{uid}")
                                    st.toast(f"❌ Dismissed action for `{target_svc}`", icon="🛑")
                                    st.session_state.action_history.insert(0, {
                                        "time": datetime.now().strftime("%H:%M:%S"),
                                        "status": "rejected",
                                        "service": target_svc,
                                        "details": f"Dismissed restart for {target_svc}.",
                                        "reason": decision.get("reason", "")
                                    })
                                    time.sleep(0.3)
                                    st.rerun()
            else:
                st.error("Failed to load queue from API.")
        except requests.exceptions.ConnectionError:
            st.error("Cannot connect to Agent API.")

    render_watchdog_queue()

    st.markdown("---")

    # Collapsible Action History (Keeps main chat clean!)
    with st.expander("📜 Remediation History", expanded=False):
        if not st.session_state.action_history:
            st.caption("No remediation actions taken yet.")
        else:
            for act in st.session_state.action_history:
                icon = "✅ Approved" if act["status"] == "approved" else "🛑 Rejected"
                st.markdown(f"**{icon}:** `{act['service']}` at `{act['time']}`")
                st.caption(f"{act['details']}")
                st.divider()

    if st.button("🧹 Clear Chat History", use_container_width=True):
        st.session_state.pending_chat_future = None
        st.session_state.messages = [{"role": "assistant", "content": "Chat cleared. Cluster monitoring active. How can I help?"}]
        st.rerun()

# --- MAIN CHAT ---
st.title("🤖 Dual-System Agents Chat")
st.write("Ask questions about cluster logs, analyze anomalies, or request autonomous remediation.")

# Render previous messages
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# Chat input (non-blocking: submits to background thread so Watchdog sidebar works in parallel!)
if prompt := st.chat_input("Ask a question about logs, or tell me to 'restart order-service'"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    action_keywords = ["restart", "fix", "remediate", "scale"]
    is_action = any(kw in prompt.lower() for kw in action_keywords)

    st.session_state.pending_chat_future = st.session_state.chat_executor.submit(
        _execute_chat_query, prompt, is_action
    )


@st.fragment(run_every=1)
def render_pending_chat():
    future = st.session_state.get("pending_chat_future")
    if future is not None:
        if not future.done():
            with st.chat_message("assistant"):
                st.info("⏳ Analyzing cluster observability data... *(You can still Approve/Reject Watchdog actions in parallel!)*")
        else:
            status, payload = future.result()
            st.session_state.pending_chat_future = None
            st.session_state.messages.append({"role": "assistant", "content": payload})
            st.rerun()


render_pending_chat()
