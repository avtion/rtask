# RTask

RTask 是 Golang 一款基于 Redis 和 Cron 的简易任务队列。

## 快速上手

您需要使用 Go Module 导入 RTask 工具包。

```shell
go get -u github.com/avtion/rtask
```

## 使用教程

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/avtion/rtask"
	"github.com/go-redis/redis/v8"
)

func main() {
	// set your job handler.
	jobHandler := func(ctx context.Context, currentJob *rtask.Job) error {
		log.Printf("process job, id: %s, payload: %s", currentJob.ID, string(currentJob.Payload))
		return nil
	}
	timeoutJobHandler := func(ctx context.Context, currentJob *rtask.Job) error {
		log.Printf("process timeout job, id: %s , payload: %s, beginAt: %s, ttl: %s",
			currentJob.ID, string(currentJob.Payload), currentJob.BeginAt, currentJob.TTL,
		)
		return nil
	}

	// rtask need redis, connect to redis first.
	rc := redis.NewClient(&redis.Options{})
	
	// then build a new task controller.
	tc, err := rtask.NewTaskController(rc, time.Minute,
		// rtask.WithBlock(),
		rtask.WithRedisKey("testRTask"),
		rtask.WithJobHandler(jobHandler),
		rtask.WithTimeoutJobHandler(timeoutJobHandler),
	)
	if err != nil {
		log.Fatalln(err)
	}

	// add some job need to do.
	_, _ = tc.AddJob(time.Now().Add(time.Minute), []byte("after one min"))
	_, _ = tc.AddJob(time.Now().Add(time.Minute), []byte("after one min, but timeout"), rtask.WithTTL(time.Millisecond))
	_, _ = tc.AddJob(time.Now().Add(2*time.Minute), []byte("after two min"))
	
	// run the task handler.
	tc.Run()
}

```

## 许可协议

MIT License