package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"context"
)

type JobStatus string

const (
	Pending  JobStatus = "PENDING"
	Running  JobStatus = "RUNNING"
	Success  JobStatus = "SUCCESS"
	Failed   JobStatus = "FAILED"
	Retrying JobStatus = "RETRYING"
)

type Job struct {
	ID       int               `json:"id"`
	Type     string            `json:"type"`
	Payload  map[string]string `json:"payload"`
	Status   JobStatus         `json:"status"`
	Attempts int               `json:"attempts"`
	MaxRetry int               `json:"max_retry"`
	Cancel   context.CancelFunc `json:"-"`
}

func NewJob(id int) *Job {
	jobTypes := []string{"email", "sms", "report"}
	jobType := jobTypes[id%len(jobTypes)]
	payload := map[string]string{"to": fmt.Sprintf("user%d@example.com", id)}
	return &Job{
		ID:       id,
		Type:     jobType,
		Payload:  payload,
		Status:   Pending,
		Attempts: 0,
		MaxRetry: 3,
	}
}

func (j *Job) String() string {
	b, _ := json.Marshal(j)
	return string(b)
}

// Global job map
var jobMap = make(map[int]*Job)
var JobMutex sync.RWMutex
