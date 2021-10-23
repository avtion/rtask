package rtask

import (
	"bytes"
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
)

type Job struct {
	// 任务的唯一标识, 根据 Payload 和 BeginAt 决定
	ID string `msgpack:"id"`
	// 任务载荷
	Payload []byte `msgpack:"payload"`
	// 任务开始运行时间
	BeginAt time.Time `msgpack:"beginAt"`
	// 任务超时时间, 如果轮询任务执行时间大于任务开始时间加超时时间, 任务会失效
	// 如果 TTL 等于 0 则认为是任务不会超时, 直至成功为止
	TTL time.Duration `msgpack:"ttl"`
}

func (j *Job) reset() *Job {
	j.ID = ""
	j.Payload = []byte{}
	j.BeginAt = time.Time{}
	j.TTL = time.Duration(0)
	return j
}

func (j *Job) generateID() error {
	beginAtBinary, err := j.BeginAt.MarshalBinary()
	if err != nil {
		return err
	}
	payloadBinary := bytes.NewBuffer(j.Payload)
	if _, err := payloadBinary.Write(beginAtBinary); err != nil {
		return err
	}
	idFields := []string{
		strconv.FormatInt(j.BeginAt.Unix(), 10),
		strconv.FormatUint(xxhash.Sum64(j.Payload), 10),
	}
	j.ID = strings.Join(idFields, ":")
	return nil
}

type JobOption func(j *Job) error

// WithTTL set job ttl.
func WithTTL(ttl time.Duration) JobOption {
	return func(j *Job) error {
		j.TTL = ttl
		return nil
	}
}
