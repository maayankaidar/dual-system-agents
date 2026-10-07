import streamlit as st
import requests
import os
AGENT_API_URL = os.getenv("AGENT_API_URL", "http://localhost:8000")

st.set_page_config(page_title="Dual-System Agents", page_icon="🤖")

# --- SIDEBAR: Watchdog Approvals ---
with st.sidebar:
    st.header("🛡️ Watchdog Approvals")
    st.write("Pending autonomous actions:")
    if st.button("Refresh Queue"):
        pass
    
    try:
        res = requests.get(f"{AGENT_API_URL}/agent/pending")
        if res.status_code == 200:
            pending = res.json()
            if not pending:
                st.success("No pending actions.")
            for uid, decision in pending.items():
                with st.expander(f"⚠️ Restart: {decision.get('target_service')}", expanded=True):
                    st.write(f"**Agent:** {decision.get('agent', 'System 1 Watchdog (0.5B)')}")
                    st.write(f"**Reason:** {decision.get('reason')}")
                    st.write(f"**Trigger:** {decision.get('incident_description')}")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("✅ Approve", key=f"approve_{uid}"):
                            app_res = requests.post(f"{AGENT_API_URL}/agent/approve/{uid}")
                            if app_res.status_code == 200 and "error" not in app_res.json():
                                st.success("Action Approved!")
                            else:
                                st.error(app_res.json().get("error", "Unknown error"))
                            st.rerun()
                    with col2:
                        if st.button("❌ Reject", key=f"reject_{uid}"):
                            requests.post(f"{AGENT_API_URL}/agent/reject/{uid}")
                            st.rerun()
    except requests.exceptions.ConnectionError:
        st.error("Cannot connect to API.")

# --- MAIN CHAT ---
st.title("🤖 Dual-System Agents Chat")
st.write("Talk to the AI agent. It can analyze logs, or fix the system if you ask it to 'restart' or 'fix' something.")

if "messages" not in st.session_state:
    st.session_state.messages = [{"role": "assistant", "content": "Hi! I'm monitoring the cluster. How can I help?"}]

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

if prompt := st.chat_input("Ask a question about logs, or tell me to 'restart log-generator'"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    action_keywords = ["restart", "fix", "remediate", "scale"]
    is_action = any(kw in prompt.lower() for kw in action_keywords)

    with st.chat_message("assistant"):
        with st.spinner("Agent is thinking..."):
            try:
                if is_action:
                    res = requests.post(
                        f"{AGENT_API_URL}/agent/remediate", 
                        json={"incident_description": prompt, "target_namespace": "streaming"}
                    )
                    if res.status_code == 200:
                        resp = res.json()
                        decision = resp.get("decision", {})
                        if resp.get("status") == "pending_approval":
                            answer = f"**Agent:** {decision.get('agent', 'System 2 Chat (7B)')}\n\n**Recommendation:** `{decision.get('action')}` on `{decision.get('target_service')}`\n\n**Reasoning:** {decision.get('reason')}\n\n*Action added to Watchdog Queue in the sidebar for approval.*"
                        else:
                            answer = f"**Agent:** {decision.get('agent', 'System 2 Chat (7B)')}\n\n**Action taken:** `{decision.get('action')}` on `{decision.get('target_service')}`\n\n**Reasoning:** {decision.get('reason')}"
                        st.markdown(answer)
                        st.session_state.messages.append({"role": "assistant", "content": answer})
                        st.rerun()
                    else:
                        st.error(f"Backend Error: {res.text}")
                else:
                    res = requests.post(f"{AGENT_API_URL}/agent/query", json={"question": prompt})
                    if res.status_code == 200:
                        data = res.json()
                        agent_name = data.get("agent", "System 2 Chat (7B)")
                        answer_text = data.get("answer", "No answer found.")
                        answer = f"**Agent:** {agent_name}\n\n{answer_text}"
                        st.markdown(answer)
                        st.session_state.messages.append({"role": "assistant", "content": answer})
                    else:
                        st.error(f"Backend Error: {res.text}")
            except requests.exceptions.ConnectionError:
                st.error("⚠️ Cannot connect to the Agent API on localhost:8000. Please make sure you run: `kubectl port-forward svc/agent-api -n lakehouse 8000:8000` in another terminal.")

