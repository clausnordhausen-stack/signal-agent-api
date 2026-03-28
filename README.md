# 🧠 Signal Agent API

### Controlled Decision System for Automated Execution

---

## 🚀 Overview

The **Signal Agent API** is a production-grade backend system designed to:

* ingest external signals
* transform them into structured decisions
* enforce risk constraints
* control execution across multiple clients

👉 This is **not a trading bot**
👉 It is a **controlled decision system**

---

## 🧠 Core Idea

Most automation systems focus on execution.

This system focuses on **decision control**.

Every incoming signal is:

1. validated
2. filtered
3. risk-checked
4. approved or rejected
5. tracked and fed back into the system

---

## 🏗️ Architecture

```mermaid
flowchart TD

A[External Signal (TradingView / API)] --> B[Signal Ingestion]

B --> C[Normalization Layer]
C --> D[Decision Engine]

D --> E[Gate System]
D --> F[AI Decision Layer]

E --> G[Execution Approval]

F --> G

G --> H[Execution Clients (MT5)]

H --> I[Deals / Results]

I --> J[KPI Engine]

J --> K[Auto Gate System]

K --> E
```

---

## ⚙️ Core Components

### 1. Signal Engine

* accepts external signals (`/tv`)
* normalizes input (BUY / SELL logic)
* deduplicates repeated signals
* ensures one active signal per symbol

---

### 2. Decision Engine

* transforms signals into executable decisions
* enforces deterministic validation
* prevents uncontrolled execution

---

### 3. Gate System

* dynamic system state: **GREEN / YELLOW / RED**
* controls whether execution is allowed
* adjusts risk exposure

---

### 4. Risk Engine

* daily loss limits
* R-multiple tracking
* trade limits per day
* margin constraints

---

### 5. Execution Layer

* distributes decisions to multiple clients
* enforces:

  * one signal → controlled execution
  * no duplicate trades
  * cooldown logic

---

### 6. KPI Engine

* tracks:

  * drawdown
  * winrate
  * loss streak
  * R performance

---

### 7. Auto Gate System

* dynamically adjusts system behavior based on KPIs

Example:

* high drawdown → reduce risk
* loss streak → block new trades

---

## 🤖 AI Decision Layer

This system integrates AI as a **bounded reasoning component**.

AI is used to:

* interpret complex signal structures
* support decision scoring
* enhance filtering logic

AI is NOT allowed to:

* execute trades directly
* override risk constraints
* bypass validation logic

👉 All AI output is validated by deterministic rules

---

## 🔁 Signal Lifecycle

```text
Signal received
   ↓
Normalized
   ↓
Checked (duplicate / cooldown)
   ↓
Evaluated (Decision Engine + AI)
   ↓
Validated (Gate + Risk)
   ↓
Approved / Rejected
   ↓
Executed (client)
   ↓
Tracked (deals + KPIs)
   ↓
Feedback into system (Auto Gate)
```

---

## 🔐 Key Design Principles

* deterministic core logic
* strict separation of concerns
* full traceability (signal → decision → trade → KPI)
* no black-box execution
* system-level risk control
* multi-client scalability

---

## 🌍 Multi-Client Architecture

The system supports:

* multiple accounts
* multiple strategies
* independent execution contexts

Each client:

* receives signals independently
* acknowledges execution (`/ack`)
* is tracked individually

---

## 📡 API Endpoints (Core)

| Endpoint             | Purpose              |
| -------------------- | -------------------- |
| `/tv`                | Signal ingestion     |
| `/latest`            | Fetch decision       |
| `/ack`               | Confirm execution    |
| `/status/gate_combo` | System state         |
| `/risk`              | Risk tracking        |
| `/deal`              | Trade reporting      |
| `/hb`                | Heartbeat monitoring |

---

## ⚙️ Tech Stack

* Python 3.10+
* FastAPI
* SQLite
* REST API architecture
* OpenAI API (AI decision layer)

---

## ▶️ Run Locally

```bash
pip install -r requirements.txt
uvicorn app:app --reload
```

---

## 📊 What Makes This System Different

This system does NOT automate blindly.

It introduces:

* controlled decision-making
* enforceable risk constraints
* system-level governance

👉 Every action is explainable
👉 Every decision is traceable

---

## 🧠 Business Perspective

This architecture is designed for:

* scalable automation systems
* AI-assisted decision platforms
* regulated environments
* multi-client deployment

---

## 🔗 Related Projects

* Trading Dashboard API
* Trading Systems Dashboard (Flutter)

---

## 📫 Contact

Claus Nordhausen
📧 [claus@nordhausen.me](mailto:claus@nordhausen.me)
