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
		fmt.Println(time.Now(), task.Name, task.Expr, "executing")
		time.Sleep(5 * time.Minute)
		return nil
	})
	job.AddJob("work2", "@every 20s", "{}")
	job.RegisterHandleFunc("work2", func(ctx context.Context, task GoodTask) error {
		fmt.Println(time.Now(), task.Name, task.Expr, "executing")
		time.Sleep(60 * time.Second)
		return nil
	})
	job.Start()

	ch := make(chan int, 0)
	<-ch
}
