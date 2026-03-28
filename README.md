# 🧠 Signal Agent API

### Controlled Decision System for Automated Execution

---

## 🚀 Overview

The **Signal Agent API** is a production-grade backend system that transforms external input into **controlled, traceable and risk-managed decisions**.

The system is designed to:

* ingest signals from external sources
* process and validate decision logic
* enforce strict risk constraints
* distribute execution across multiple clients

👉 This is **not a trading bot**
👉 This is a **decision system with execution control**

---

## 🧠 Core Idea

Most systems automate execution.

This system controls **decision quality**.

Every signal passes a structured pipeline:

1. ingestion
2. normalization
3. validation
4. decision logic
5. risk control
6. execution approval
7. tracking and feedback

---

## 🏗️ System Flow

```text
External Signal
   ↓
Signal Ingestion
   ↓
Normalization Layer
   ↓
Decision Engine
   ↓
Gate System + AI Layer
   ↓
Execution Approval
   ↓
Execution Clients (MT5)
   ↓
Deals / Results
   ↓
KPI Engine
   ↓
Auto Gate System
   ↓
Feedback into system
```

---

## ⚙️ Core Components

### 1. Signal Engine

* accepts external signals (`/tv`)
* normalizes input (BUY / SELL logic)
* removes duplicates
* enforces one active signal per symbol

---

### 2. Decision Engine

* transforms signals into structured decisions
* applies deterministic logic
* prevents uncontrolled execution

---

### 3. Gate System

* system states: **GREEN / YELLOW / RED**
* controls if execution is allowed
* dynamically adjusts risk exposure

---

### 4. Risk Engine

* daily loss limits
* R-multiple tracking
* max trades per day
* margin constraints

---

### 5. Execution Layer

* distributes decisions to multiple clients
* enforces:

  * one signal → one controlled execution
  * no duplicate trades
  * cooldown logic

---

### 6. KPI Engine

* tracks performance:

  * drawdown
  * winrate
  * loss streak
  * R performance

---

### 7. Auto Gate System

* adjusts system behavior dynamically

Example:

* high drawdown → reduce risk
* loss streak → block new trades

---

## 🤖 AI Decision Layer

The system integrates AI as a **bounded and controlled reasoning component**.

AI is used to:

* interpret complex inputs
* support decision scoring
* enhance rule-based filtering

AI is NOT allowed to:

* execute trades
* override risk constraints
* bypass validation logic

👉 Final decisions are always enforced by deterministic rules

---

## 🔁 Signal Lifecycle

```text
Signal received
   ↓
Normalized
   ↓
Duplicate / cooldown check
   ↓
Decision evaluation (rules + AI)
   ↓
Risk validation
   ↓
Approved / Rejected
   ↓
Executed
   ↓
Tracked (deals + KPIs)
   ↓
Feedback into system
```

---

## 🔐 Design Principles

* deterministic core logic
* strict separation of concerns
* full traceability
* no black-box execution
* system-level risk governance
* scalable multi-client architecture

---

## 🌍 Multi-Client Architecture

The system supports:

* multiple trading accounts
* independent execution per client
* per-client signal acknowledgment

Each client:

* receives signals independently
* confirms execution (`/ack`)
* is tracked individually

---

## 📡 API Endpoints

| Endpoint             | Purpose               |
| -------------------- | --------------------- |
| `/tv`                | Signal ingestion      |
| `/latest`            | Fetch latest decision |
| `/ack`               | Confirm execution     |
| `/status/gate_combo` | System state          |
| `/risk`              | Risk tracking         |
| `/deal`              | Trade reporting       |
| `/hb`                | Heartbeat monitoring  |

---

## ⚙️ Tech Stack

* Python 3.10+
* FastAPI
* SQLite
* REST architecture
* OpenAI API

---

## ▶️ Run Locally

```bash
pip install -r requirements.txt
uvicorn app:app --reload
```

---

## 📊 What Makes This System Different

This system does not automate blindly.

It introduces:

* structured decision pipelines
* enforceable risk constraints
* full transparency
* controlled execution

👉 Every action is explainable
👉 Every decision is traceable

---

## 🧠 Business Perspective

The system is designed for:

* scalable automation platforms
* AI-assisted decision systems
* multi-client environments
* controlled execution scenarios

---

## 🔗 Related Projects

* Trading Dashboard API
* Trading Systems Dashboard (Flutter)

---

## 📫 Contact

Claus Nordhausen
📧 [claus@nordhausen.me](mailto:claus@nordhausen.me)
