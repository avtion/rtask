package rtask

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

func newTestTaskCenter(opts ...BuildTaskControllerOption) (*TaskController, error) {
	rc := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS"),
		DB:   1,
	})
	if err := rc.Ping(context.TODO()).Err(); err != nil {
		return nil, err
	}
	tc, err := NewTaskController(rc, 3*time.Second, opts...)
	if err != nil {
		return nil, err
	}
	return tc, nil
}

func TestAddJob(t *testing.T) {
	tc, err := newTestTaskCenter()
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("hello world")
	jobID, err := tc.AddJob(time.Now().Add(1*time.Minute), payload)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("new job add, id: %s", jobID)
}

func TestListJob(t *testing.T) {
	tc, err := newTestTaskCenter()
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := tc.ListJobs()
	if err != nil {
		t.Fatal(err)
	}
	for _, job := range jobs {
		t.Logf("id: %s, payload: %s", job.ID, string(job.Payload))
	}
}

func TestRemJob(t *testing.T) {
	tc, err := newTestTaskCenter()
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("hello world")
	jobID, err := tc.AddJob(time.Now().Add(1*time.Minute), payload)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("add job, id: %s", jobID)
	affected, err := tc.RemoveJobs(jobID)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("remove job successfully, affected: %d", affected)
}

func TestRunTask(t *testing.T) {
	jobHandler := func(ctx context.Context, currentJob *Job) error {
		t.Logf("process job, id: %s, payload: %s", currentJob.ID, string(currentJob.Payload))
		return nil
	}
	timeoutJobHandler := func(ctx context.Context, currentJob *Job) error {
		t.Logf("process timeout job, id: %s , payload: %s, beginAt: %s, ttl: %s",
			currentJob.ID, string(currentJob.Payload), currentJob.BeginAt, currentJob.TTL,
		)
		return nil
	}
	tc, err := newTestTaskCenter(
		WithBlock(),
		WithRedisKey("testRTask"),
		WithJobHandler(jobHandler),
		WithTimeoutJobHandler(timeoutJobHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = tc.AddJob(time.Now().Add(4*time.Second), []byte("hello 1"))
	_, _ = tc.AddJob(time.Now().Add(7*time.Second), []byte("hello 2"), WithTTL(time.Millisecond))
	_, _ = tc.AddJob(time.Now().Add(10*time.Second), []byte("hello 3"))
	TestListJob(t)
	tc.Run()
}
