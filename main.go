package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"
)

func main() {
	log.Println("🚀 Starting Go Job Simulator with Web Dashboard")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	// Initialize worker pool
	pool := NewPool(3)
	pool.Start(ctx)

	// Load unfinished jobs from WAL
	jobs := LoadJobsFromWAL()
	JobMutex.Lock()
	for _, job := range jobs {
		jobMap[job.ID] = job
	}
	JobMutex.Unlock()

	for _, job := range jobs {
		log.Printf("🔄 Re-queueing job from WAL: %v\n", job)
		pool.Submit(job)
	}

	// Auto job generator
	go func() {
		jobID := len(jobs) + 1
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(500+randInt(500)) * time.Millisecond):
				if autoGeneratorActive {
					job := NewJob(jobID)
					JobMutex.Lock()
					jobMap[jobID] = job
					JobMutex.Unlock()
					pool.Submit(job)
					AppendJobToWAL(job)
					jobID++
				}
			}
		}
	}()

	// Terminal monitor (refresh table every 1.5s)
	go monitor(ctx)

	// Checkpointer (Compaction)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
				Checkpoint()
			}
		}
	}()

	// HTTP server
	http.Handle("/", http.FileServer(http.Dir("./web")))
	http.HandleFunc("/jobs", handleJobSubmit(pool))
	http.HandleFunc("/jobs/status", handleJobStatus(pool))
	http.HandleFunc("/api/reset", handleReset)
	http.HandleFunc("/api/toggle-auto", handleToggleAuto)
	http.HandleFunc("/api/jobs/stop", handleJobStop)
	http.HandleFunc("/api/pool/resize", handlePoolResize(pool, ctx))
	http.HandleFunc("/api/logs", func(w http.ResponseWriter, r *http.Request) {
		b, _ := os.ReadFile("jobs.log")
		w.Header().Set("Content-Type", "text/plain")
		w.Write(b)
	})
	srv := &http.Server{Addr: ":8080"}

	go func() {
		log.Println("🌐 Web dashboard listening on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	log.Println("⚡ Shutting down gracefully...")
	pool.Stop()
	srv.Shutdown(context.Background())
	log.Println("✅ All workers stopped. Exiting.")
}

func handleJobSubmit(pool *Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var job Job
		if err := json.NewDecoder(r.Body).Decode(&job); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		job.Status = Pending
		job.Attempts = 0
		if job.MaxRetry == 0 {
			job.MaxRetry = 3
		}
		JobMutex.Lock()
		jobMap[job.ID] = &job
		JobMutex.Unlock()
		pool.Submit(&job)
		AppendJobToWAL(&job)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(job)
		log.Printf("📩 Received job via HTTP: %v\n", job)
	}
}

func handleJobStatus(pool *Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		JobMutex.RLock()
		statuses := make([]*Job, 0, len(jobMap))
		for _, job := range jobMap {
			statuses = append(statuses, job)
		}
		JobMutex.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		
		response := map[string]interface{}{
			"jobs": statuses,
			"auto": autoGeneratorActive,
			"poolSize": pool.Size(),
		}
		json.NewEncoder(w).Encode(response)
	}
}

var autoGeneratorActive = true

func handleReset(w http.ResponseWriter, r *http.Request) {
	JobMutex.Lock()
	jobMap = make(map[int]*Job)
	JobMutex.Unlock()
	os.WriteFile("jobs.log", []byte(""), 0644)
	w.WriteHeader(http.StatusOK)
}

func handleToggleAuto(w http.ResponseWriter, r *http.Request) {
	autoGeneratorActive = !autoGeneratorActive
	w.WriteHeader(http.StatusOK)
}

func handleJobStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	JobMutex.RLock()
	job, ok := jobMap[req.ID]
	JobMutex.RUnlock()

	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	JobMutex.Lock()
	if job.Status == Running && job.Cancel != nil {
		job.Cancel()
	}
	JobMutex.Unlock()

	w.WriteHeader(http.StatusOK)
}

func handlePoolResize(pool *Pool, ctx context.Context) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Size int `json:"size"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if req.Size > 0 {
			pool.Resize(ctx, req.Size)
		}
		w.WriteHeader(http.StatusOK)
	}
}
