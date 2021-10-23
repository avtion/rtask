package rtask

import (
	"context"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/robfig/cron/v3"
	"github.com/vmihailenco/msgpack/v5"
)

// Run begin process task.
func (tc *TaskController) Run() {
	// https://pkg.go.dev/github.com/robfig/cron#ConstantDelaySchedule
	tc.cronEntryID = tc.cc.Schedule(cron.Every(tc.interval), cron.FuncJob(tc.intervalEvent))
	if tc.isBlockGoroutine {
		tc.cc.Run()
	} else {
		tc.cc.Start()
	}
}

// RemoveJobs remove jobs.
func (tc *TaskController) RemoveJobs(jobIDs ...string) (int64, error) {
	if len(jobIDs) == 0 {
		return 0, nil
	}
	idMapping := make(map[string]struct{}, len(jobIDs))
	for _, id := range jobIDs {
		idMapping[id] = struct{}{}
	}
	jobToRem := make([]interface{}, 0, len(jobIDs))
	ctx := context.TODO()
	result, err := tc.rc.ZRange(ctx, tc.redisKey, 0, -1).Result()
	if err != nil {
		return 0, err
	}
	job := tc.jobPool.Get().(*Job)
	defer tc.jobPool.Put(job)
	for _, jobStr := range result {
		job.reset()
		if err := msgpack.Unmarshal([]byte(jobStr), job); err != nil {
			return 0, err
		}
		if _, isNeedToDelete := idMapping[job.ID]; isNeedToDelete {
			jobToRem = append(jobToRem, jobStr)
		}
	}
	affected, err := tc.rc.ZRem(ctx, tc.redisKey, jobToRem...).Result()
	if err != nil {
		return 0, err
	}
	return affected, nil
}

// ListJobs list all jobs.
func (tc *TaskController) ListJobs() ([]*Job, error) {
	ctx := context.TODO()
	result, err := tc.rc.ZRange(ctx, tc.redisKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}
	var jobList = make([]*Job, 0)
	for _, val := range result {
		job := new(Job)
		if err := msgpack.Unmarshal([]byte(val), job); err != nil {
			return nil, err
		}
		jobList = append(jobList, job)
	}
	return jobList, nil
}

// AddJob add a new job to task controller.
func (tc *TaskController) AddJob(beginAt time.Time, payload []byte, opts ...JobOption) (string, error) {
	if beginAt.Before(time.Now().Add(tc.interval)) {
		return "", errors.New("add job failed, handle job begin time before current time add interval")
	}
	j := tc.jobPool.Get().(*Job).reset()
	defer tc.jobPool.Put(j)
	j.Payload = payload
	j.BeginAt = beginAt
	// option can not change job id
	for _, opt := range opts {
		if err := opt(j); err != nil {
			return "", err
		}
	}
	if err := j.generateID(); err != nil {
		return "", err
	}
	jobIsExist, err := tc.isJobExist(j.ID)
	if err != nil {
		return "", err
	}
	if jobIsExist {
		return j.ID, nil
	}
	jobMarshalData, err := msgpack.Marshal(j)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), tc.interval)
	defer cancel()
	_, err = tc.rc.ZAddNX(ctx, tc.redisKey, &redis.Z{
		Score:  float64(j.BeginAt.Unix()),
		Member: jobMarshalData,
	}).Result()
	return j.ID, err
}
