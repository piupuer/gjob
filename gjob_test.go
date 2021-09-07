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
	err = job.AddJob("work", "@every 10s", "{}", func(ctx context.Context, task GoodTask) error {
		fmt.Println(time.Now(), task.Name, task.Expr, "executing")
		time.Sleep(30 * time.Second)
		return nil
	}).Start()
	if err != nil {
		panic(err)
	}

	err = job.AddJob("work2", "@every 20s", "{}", func(ctx context.Context, task GoodTask) error {
		fmt.Println(time.Now(), task.Name, task.Expr, "executing")
		time.Sleep(15 * time.Second)
		return nil
	}).Start()
	if err != nil {
		panic(err)
	}
	time.Sleep(50 * time.Second)
	err = job.Stop("work2")
	if err != nil {
		panic(err)
	}

	ch := make(chan int, 0)
	<-ch
}
