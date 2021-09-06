package gjob

import (
	"context"
	"github.com/hibiken/asynq"
	"sync"
	"time"
)

type Config struct {
	RedisUri string
}

type GoodJob struct {
	lock      sync.Mutex
	server    *asynq.Server
	scheduler *asynq.Scheduler
	redis     asynq.RedisConnOpt
	tasks     map[string]GoodTask
	handles   map[string]func(context.Context, GoodTask) error
	entries   map[string]string
}

type GoodTask struct {
	Name string
	Json string
	t    *asynq.Task
}

func New(cfg Config) (*GoodJob, error) {
	if cfg.RedisUri == "" {
		cfg.RedisUri = "redis://127.0.0.1:6379/0"
	}
	opt, err := asynq.ParseRedisURI(cfg.RedisUri)
	if err != nil {
		return nil, err
	}

	job := GoodJob{
		redis: opt,
	}
	job.tasks = make(map[string]GoodTask)
	job.handles = make(map[string]func(context.Context, GoodTask) error)
	job.entries = make(map[string]string)
	return &job, nil
}

func (g GoodJob) AddJob(taskName, expr, taskJson string) GoodJob {
	g.lock.Lock()
	defer g.lock.Unlock()
	task := asynq.NewTask(taskName, []byte(taskJson))
	g.tasks[expr] = GoodTask{
		Name: task.Type(),
		Json: string(task.Payload()),
		t:    task,
	}
	return g
}

func (g GoodJob) RegisterHandleFunc(taskName string, handler func(context.Context, GoodTask) error) GoodJob {
	g.lock.Lock()
	defer g.lock.Unlock()
	g.handles[taskName] = handler
	return g
}

func (g GoodJob) Start() error {
	g.lock.Lock()
	defer g.lock.Unlock()
	if len(g.tasks) == 0 {
		return nil
	}
	if len(g.handles) == 0 {
		return nil
	}
	scheduler := asynq.NewScheduler(
		g.redis,
		&asynq.SchedulerOpts{
			Logger:              nil,
			LogLevel:            asynq.DebugLevel,
			Location:            time.Local,
			EnqueueErrorHandler: nil,
		},
	)
	server := asynq.NewServer(
		g.redis,
		asynq.Config{
			Concurrency:         10,
			RetryDelayFunc:      nil,
			IsFailure:           nil,
			Queues:              nil,
			StrictPriority:      false,
			ErrorHandler:        nil,
			Logger:              nil,
			LogLevel:            0,
			ShutdownTimeout:     0,
			HealthCheckFunc:     nil,
			HealthCheckInterval: 0,
		},
	)
	h := asynq.NewServeMux()
	for name, handle := range g.handles {
		for expr, task := range g.tasks {
			if task.Name == name {
				entryId, err := scheduler.Register(expr, task.t)
				if err != nil {
					return err
				}
				g.entries[name] = entryId
				h.HandleFunc(name, func(ctx context.Context, t *asynq.Task) error {
					task := GoodTask{
						Name: t.Type(),
						Json: string(t.Payload()),
						t:    t,
					}
					return handle(ctx, task)
				})
			}
		}
	}
	go server.Run(h)
	go scheduler.Run()
	g.server = server
	g.scheduler = scheduler
	return nil
}
