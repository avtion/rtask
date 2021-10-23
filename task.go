package rtask

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/robfig/cron/v3"
	"github.com/vmihailenco/msgpack/v5"
)

const defaultTaskControllerKey = "taskcontroller"

type TaskController struct {
	rc      *redis.Client
	cc      *cron.Cron
	jobPool sync.Pool

	// 轮询任务的 EntryID
	cronEntryID cron.EntryID
	// 轮询任务时间间隔, Job 开始运行时间必须大于当前时间加 interval
	interval time.Duration
	// 轮询任务是否阻塞协程
	isBlockGoroutine bool
	// KV 存储 Job 时所需的键值, 默认为 taskcenter, 可以通过 BuildTaskControllerOption 进行修改
	redisKey string

	// 任务处理函数, 任务中心将会调用该函数处理所有的 Job
	JobHandler func(ctx context.Context, currentJob *Job) error
	// 超时任务处理函数
	TimeoutJobHandler func(ctx context.Context, currentJob *Job) error
}

type BuildTaskControllerOption func(tc *TaskController) error

// NewTaskController build a new task controller.
func NewTaskController(rc *redis.Client, interval time.Duration, opts ...BuildTaskControllerOption) (*TaskController, error) {
	tc := &TaskController{
		rc:       rc,
		cc:       cron.New(),
		jobPool:  sync.Pool{New: func() interface{} { return new(Job) }},
		interval: interval,
		redisKey: defaultTaskControllerKey,
	}
	for _, opt := range opts {
		if err := opt(tc); err != nil {
			return nil, err
		}
	}
	return tc, nil
}

func (tc *TaskController) intervalEvent() {
	ctx, cancel := context.WithTimeout(context.Background(), tc.interval)
	defer cancel()
	result, err := tc.rc.ZRangeByScore(ctx, tc.redisKey, &redis.ZRangeBy{
		Min: "",
		Max: strconv.FormatInt(time.Now().Unix(), 10),
	}).Result()
	if err != nil {
		return
	}
	if len(result) == 0 {
		return
	}
	for _, val := range result {
		if err := tc.processJob(val); err != nil {
			continue
		}
	}
}

func (tc *TaskController) processJob(jobMarshalData string) error {
	job := tc.jobPool.Get().(*Job).reset()
	defer tc.jobPool.Put(job)
	if err := msgpack.Unmarshal([]byte(jobMarshalData), job); err != nil {
		return err
	}
	if job.TTL > 0 && job.BeginAt.Add(job.TTL).Before(time.Now()) {
		_, _ = tc.RemoveJobs(job.ID)
		if tc.TimeoutJobHandler != nil {
			return tc.TimeoutJobHandler(context.Background(), job)
		}
		return fmt.Errorf("process job failed, beacuse job is out of time, id: %s, beginAt: %s, ttl: %s",
			job.ID, job.BeginAt.String(), job.TTL.String())
	}
	if tc.JobHandler == nil {
		// if task center has no job handler, it will remove job ignore any case
		_, _ = tc.RemoveJobs(job.ID)
		return nil
	}
	if err := tc.JobHandler(context.TODO(), job); err != nil {
		// keep the job in task
		return err
	}
	_, _ = tc.RemoveJobs(job.ID)
	return nil
}

func (tc *TaskController) isJobExist(id string) (bool, error) {
	ctx := context.TODO()
	result, err := tc.rc.ZRange(ctx, tc.redisKey, 0, -1).Result()
	if err != nil {
		return false, err
	}
	job := tc.jobPool.Get().(*Job)
	defer tc.jobPool.Put(job)
	for _, val := range result {
		job.reset()
		if err := msgpack.Unmarshal([]byte(val), job); err != nil {
			return false, err
		}
		if job.ID == id {
			return true, nil
		}
	}
	return false, nil
}

// WithBlock set cron goroutine block
func WithBlock() BuildTaskControllerOption {
	return func(tc *TaskController) error {
		tc.isBlockGoroutine = true
		return nil
	}
}

// WithJobHandler set job handler.
func WithJobHandler(fn func(ctx context.Context, currentJob *Job) error) BuildTaskControllerOption {
	return func(tc *TaskController) error {
		tc.JobHandler = fn
		return nil
	}
}

// WithTimeoutJobHandler set timeout job handler.
func WithTimeoutJobHandler(fn func(ctx context.Context, currentJob *Job) error) BuildTaskControllerOption {
	return func(tc *TaskController) error {
		tc.TimeoutJobHandler = fn
		return nil
	}
}

// WithRedisKey set task controller redis key.
func WithRedisKey(key string) BuildTaskControllerOption {
	return func(tc *TaskController) error {
		tc.redisKey = key
		return nil
	}
}
