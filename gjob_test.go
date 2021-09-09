package gjob

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"
)

func Run(ctx context.Context) error {
	http.Get(fmt.Sprintf("http://106.75.132.201/api/ping?key=%d&pid=%d", time.Now().Unix(), os.Getpid()))
	return nil
}

var uri1 = "redis://:123456@106.75.132.201:6179"
var uri2 = "redis-sentinel://:123456@106.75.132.201:6182,106.75.132.201:6183,106.75.132.201:6184?master=prod"

func TestNew(t *testing.T) {
	job, err := New(Config{
		RedisUri: uri2,
	})
	if err != nil {
		panic(err)
	}
	job.AddTask(GoodTask{
		Name:    "work",
		Expr:    "@every 10s",
		Payload: "{}",
		Func: func(ctx context.Context) error {
			return Run(ctx)
		},
	}).Start()

	time.Sleep(30 * time.Second)
	job.AddTask(GoodTask{
		Name:    "work2",
		Expr:    "@every 5s",
		Payload: "{}",
		Func: func(ctx context.Context) error {
			return Run(ctx)
		},
	}).Start()
	
	time.Sleep(15 * time.Second)
	job.Stop("work")

	ch := make(chan int, 0)
	<-ch
}
