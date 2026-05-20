package main

import (
	"context"
	"log"
	"sync"
	"time"
)

type Pool struct {
	jobChan       chan *Job
	wg            sync.WaitGroup
	size          int
	mu            sync.Mutex
	stopChan      chan struct{}
	workerCounter int
}

func NewPool(size int) *Pool {
	return &Pool{
		jobChan:       make(chan *Job, 100),
		size:          size,
		stopChan:      make(chan struct{}, 100),
		workerCounter: 0,
	}
}

func (p *Pool) Start(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := 1; i <= p.size; i++ {
		p.workerCounter++
		p.wg.Add(1)
		go p.worker(ctx, p.workerCounter)
	}
}

func (p *Pool) Resize(ctx context.Context, newSize int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if newSize > p.size {
		for i := 0; i < newSize-p.size; i++ {
			p.workerCounter++
			p.wg.Add(1)
			go p.worker(ctx, p.workerCounter)
		}
	} else if newSize < p.size {
		for i := 0; i < p.size-newSize; i++ {
			p.stopChan <- struct{}{}
		}
	}
	p.size = newSize
}

func (p *Pool) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.size
}

func (p *Pool) worker(ctx context.Context, id int) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopChan:
			log.Printf("🛑 Worker-%d scaling down\n", id)
			return
		case job, ok := <-p.jobChan:
			if !ok {
				return
			}
			p.processJob(id, job)
		}
	}
}

func handleFailure(workerID int, job *Job, p *Pool) {
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
}

func (p *Pool) processJob(workerID int, job *Job) {
	job.Status = Running
	job.Attempts++

	ctx, cancel := context.WithCancel(context.Background())
	JobMutex.Lock()
	job.Cancel = cancel
	JobMutex.Unlock()
	defer cancel()

	AppendJobToWAL(job)
	log.Printf("👷 Worker-%d started job: %s\n", workerID, job.String())

	// Simulate work
	workDuration := time.Duration(1+randInt(2)) * time.Second
	select {
	case <-time.After(workDuration):
		// Simulate failure
		if randInt(10) < 3 {
			handleFailure(workerID, job, p)
			return
		}
	case <-ctx.Done():
		log.Printf("🛑 Worker-%d job %d stopped externally\n", workerID, job.ID)
		handleFailure(workerID, job, p)
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