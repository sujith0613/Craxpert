# Craxpert: Go Job Simulator with WAL & Enterprise UI

Craxpert is a robust, concurrent Job Queue Simulator built in **Go (Golang)**. It features a persistent **Write-Ahead Log (WAL)** for crash-recovery, a "Steal/No-Force" background log compaction (checkpointing) mechanism, and a stunning, real-time Single Page Application (SPA) dashboard.

## 🚀 Key Features

* **Concurrent Worker Pool**: Dispatch and process multiple simulated jobs (Emails, SMS, Reports, Webhooks) simultaneously using lightweight Go routines and channels.
* **Write-Ahead Log (WAL)**: All job state transitions (`PENDING`, `RUNNING`, `SUCCESS`, `FAILED`, `RETRYING`) are instantly serialized to disk before execution, ensuring zero data loss if the server crashes.
* **Checkpointing (Log Compaction)**: Employs a background compaction worker every 10 seconds to snapshot active jobs and flush them to disk ("Steal" approach), keeping the WAL tiny and optimized for millisecond startup times.
* **Enterprise SPA Dashboard**: A beautiful, Stripe/Vercel-inspired UI built with pure HTML/CSS/JS. Features include:
  * **Job Queue**: Live-updating grid with dynamic row gradients based on job type and attempt count.
  * **Activity Timeline**: A front-end audit log tracking all state transitions in real-time.
  * **Raw Logs View**: Direct access to the `jobs.log` WAL stream straight from the browser.
  * **Live Connection Monitor**: The UI auto-detects if the backend server goes offline or reboots.
* **Automatic & Manual Dispatch**: Includes a built-in auto-generator to simulate massive traffic spikes, along with a UI modal to manually inject custom payloads.

## 🛠️ Architecture

* **Backend**: Go (`net/http`)
* **Concurrency**: Native Goroutines, Channels, `sync.RWMutex`
* **Persistence**: Append-only JSON WAL with periodic compaction
* **Frontend**: Vanilla HTML5, CSS Variables, ES6 JavaScript Fetch API

## 🚦 Getting Started

### Prerequisites
* Go 1.20+ installed and added to your system `PATH`.

### Running the Server
1. Clone the repository:
   ```bash
   git clone https://github.com/sujith0613/Craxpert.git
   cd Craxpert
   ```
2. Start the backend server:
   ```bash
   go run .
   ```
3. Open the Operations Dashboard in your browser:
   ```text
   http://localhost:8080
   ```

## 🎮 Dashboard Controls

* **Reset System**: Completely drops all jobs from memory, deletes the WAL history, and resets the simulator to zero instantly.
* **Stop/Start Auto-Gen**: Pauses the random job generator so you can observe the queue drain or manually test single injections.

## 🐛 Recent Bug Fixes & Optimizations
* Solved `5 / 0` Attempt Glitches by perfectly reconstructing the *latest* state of a job during WAL bootup and defaulting missing `MaxRetry` keys.
* Prevented memory-crash panics (`concurrent map read and write`) by implementing strict `sync.RWMutex` locking across HTTP Handlers, Workers, and the Reset controllers.
* Fixed server deadlocks caused by unbuffered channel blocks during massive WAL restores by making worker submissions entirely asynchronous.
