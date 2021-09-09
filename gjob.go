package gjob

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v7"
	"github.com/hibiken/asynq"
	"github.com/libi/dcron"
	"sync"
)

type Config struct {
	RedisUri    string
	RedisClient redis.UniversalClient
}

type GoodJob struct {
	lock   sync.Mutex
	redis  redis.UniversalClient
	driver *RedisClientDriver
	tasks  map[string]GoodTask
	Error  error
}

type GoodTask struct {
	cron       *dcron.Dcron
	running    bool
	Name       string
	Expr       string
	Payload    string
	Func       func(ctx context.Context) error
	ErrHandler func(err error)
}

func New(cfg Config) (*GoodJob, error) {
	if cfg.RedisUri == "" {
		cfg.RedisUri = "redis://127.0.0.1:6379/0"
	}
	r, err := getRedisClientFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	// init fields
	job := GoodJob{
		redis: r,
	}
	drv, err := NewDriver(job.redis)
	if err != nil {
		return nil, err
	}
	job.driver = drv
	job.tasks = make(map[string]GoodTask, 0)
	return &job, nil
}

func (g *GoodJob) AddTask(task GoodTask) *GoodJob {
	g.lock.Lock()
	defer g.lock.Unlock()
	if _, ok := g.tasks[task.Name]; ok {
		fmt.Printf("task %s already exists, skip\n", task.Name)
		return g
	}
	task.cron = dcron.NewDcron(task.Name, g.driver)
	g.tasks[task.Name] = task
	fun := (func(task GoodTask) func() {
		return func() {
			ctx := context.Background()
			err := task.Func(ctx)
			if err != nil {
				task.ErrHandler(err)
			}
		}
	})(task)
	task.cron.AddFunc(task.Name, task.Expr, fun)
	return g
}

func (g *GoodJob) Start() {
	g.lock.Lock()
	defer g.lock.Unlock()
	for _, task := range g.tasks {
		if !task.running {
			task.cron.Start()
			task.running = true
			g.tasks[task.Name] = task
		}
	}
}

func (g *GoodJob) StopAll() {
	g.lock.Lock()
	defer g.lock.Unlock()
	for _, task := range g.tasks {
		if task.running {
			task.cron.Stop()
			task.running = false
			g.tasks[task.Name] = task
		}
	}
}

func (g *GoodJob) Stop(taskName string) {
	g.lock.Lock()
	defer g.lock.Unlock()
	for _, task := range g.tasks {
		if task.running && task.Name == taskName {
			task.cron.Stop()
			task.running = false
			g.tasks[task.Name] = task
			break
		} else {
			fmt.Printf("task %s is not running, skip\n", task.Name)
		}
	}
}

func getRedisClientFromConfig(cfg Config) (redis.UniversalClient, error) {
	var opt asynq.RedisConnOpt
	var err error
	if cfg.RedisUri != "" {
		opt, err = asynq.ParseRedisURI(cfg.RedisUri)
		if err != nil {
			return nil, err
		}
		return opt.MakeRedisClient().(redis.UniversalClient), nil
	} else if cfg.RedisClient != nil {
		return cfg.RedisClient, nil
	}
	return nil, fmt.Errorf("invalid redis config")
}
