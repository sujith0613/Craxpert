package main

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"sync"
)

var walMutex sync.Mutex

func AppendJobToWAL(job *Job) {
	walMutex.Lock()
	defer walMutex.Unlock()

	file, err := os.OpenFile("jobs.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println("⚠️  Failed to open WAL:", err)
		return
	}
	defer file.Close()

	b, _ := json.Marshal(job)
	if _, err := file.WriteString(string(b) + "\n"); err != nil {
		log.Println("⚠️  Failed to write job to WAL:", err)
	}
}

// Checkpoint performs a "steal, no-force" style compaction
// It flushes active jobs to disk asynchronously without forcing a write on every transaction
func Checkpoint() {
	walMutex.Lock()
	defer walMutex.Unlock()

	JobMutex.RLock()
	activeJobs := []*Job{}
	for _, job := range jobMap {
		if job.Status != Success && job.Status != Failed {
			activeJobs = append(activeJobs, job)
		}
	}
	JobMutex.RUnlock()

	// Steal: writing dirty/active state to disk
	file, err := os.OpenFile("jobs.tmp", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Println("⚠️  Failed to create checkpoint temp file:", err)
		return
	}
	for _, job := range activeJobs {
		b, _ := json.Marshal(job)
		file.WriteString(string(b) + "\n")
	}
	file.Close()

	os.Rename("jobs.tmp", "jobs.log")
	log.Println("💾 Checkpoint created, WAL compacted.")
}

func LoadJobsFromWAL() []*Job {
	latestJobs := make(map[int]*Job)
	file, err := os.Open("jobs.log")
	if err != nil {
		return []*Job{}
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var job Job
		if err := json.Unmarshal(scanner.Bytes(), &job); err != nil {
			log.Println("⚠️  Failed to parse job from WAL:", err)
			continue
		}
		if job.MaxRetry == 0 {
			job.MaxRetry = 3
		}
		latestJobs[job.ID] = &job
	}

	jobs := []*Job{}
	for _, job := range latestJobs {
		if job.Status != Success && job.Status != Failed {
			jobs = append(jobs, job)
		}
	}
	return jobs
}
