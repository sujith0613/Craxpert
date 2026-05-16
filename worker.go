package main

import (
	"context"
	"log"
	"sync"
	"time"
)

type Pool struct {
	jobChan chan *Job
	wg      sync.WaitGroup
	size    int
}

func NewPool(size int) *Pool {
	return &Pool{
		jobChan: make(chan *Job, 100),
		size:    size,
	}
}

func (p *Pool) Start(ctx context.Context) {
	for i := 1; i <= p.size; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}
}

func (p *Pool) worker(ctx context.Context, id int) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-p.jobChan:
			p.processJob(id, job)
		}
	}
}

func (p *Pool) processJob(workerID int, job *Job) {
	job.Status = Running
	job.Attempts++
	AppendJobToWAL(job)
	log.Printf("👷 Worker-%d started job: %s\n", workerID, job.String())

	// Simulate work
	time.Sleep(time.Duration(1+randInt(2)) * time.Second)

	// Simulate failure
	if randInt(10) < 3 {
		if job.Attempts < job.MaxRetry {
			job.Status = Retrying
			AppendJobToWAL(job)
			log.Printf("⏳ Worker-%d retrying job %d in 2 seconds\n", workerID, job.ID)
			time.Sleep(2 * time.Second)
			p.Submit(job)
		} else {
			job.Status = Failed
			AppendJobToWAL(job)
			log.Printf("❌ Worker-%d failed job: %s\n", workerID, job.String())
		}
		return
	}

	job.Status = Success
	AppendJobToWAL(job)
	log.Printf("✅ Worker-%d completed job: %s\n", workerID, job.String())
}

func (p *Pool) Submit(job *Job) {
	if job.Status == Pending {
		log.Printf("📥 Job queued: %s\n", job.String())
	}
	go func() {
		p.jobChan <- job
	}()
}

func (p *Pool) Stop() {
	close(p.jobChan)
	p.wg.Wait()
}

func monitor(ctx context.Context) {
	// Terminal monitor loop
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(1500 * time.Millisecond):
			// Do nothing, UI handles display
		}
	}
}