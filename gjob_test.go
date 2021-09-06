package gjob

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	job, err := New(Config{
		RedisUri: "redis://:123456@106.75.132.201:6179",
	})
	if err != nil {
		panic(err)
	}
	job.AddJob("work", "@every 10s", "{}")
	job.RegisterHandleFunc("work", func(ctx context.Context, task GoodTask) error {
		fmt.Println("ctx", ctx)
		fmt.Println("task", task)
		time.Sleep(10 * time.Second)
		return nil
	})
	job.Start()

	ch := make(chan int, 0)
	<-ch
}
