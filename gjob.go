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
	inspector *asynq.Inspector
	redis     asynq.RedisConnOpt
	tasks     map[string]GoodTask
	handles   map[string]func(context.Context, GoodTask) error
	entries   map[string]string
}

type GoodTask struct {
	Name string
	Expr string
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
	g.tasks[taskName] = GoodTask{
		Name: task.Type(),
		Expr: expr,
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
			Concurrency:    10,
			RetryDelayFunc: nil,
			IsFailure:      nil,
			Queues: map[string]int{
				"critical": 6,
				"default":  3,
				"low":      1,
			},
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
		for taskName, task := range g.tasks {
			if taskName == name {
				entryId, err := scheduler.Register(
					task.Expr,
					task.t,
					asynq.MaxRetry(0),
					asynq.Timeout(24*time.Hour),
				)
				if err != nil {
					return err
				}
				g.entries[name] = entryId
				h.HandleFunc(name, func(ctx context.Context, t *asynq.Task) error {
					active := g.isActive(t.Type())
					if active {
						return nil
					}
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
	g.inspector = asynq.NewInspector(g.redis)
	return nil
}

func (g GoodJob) isActive(name string) bool {
	g.lock.Lock()
	defer g.lock.Unlock()
	queues, err := g.inspector.Queues()
	if err != nil {
		return false
	}
	currentTasks := make([]*asynq.TaskInfo, 0)
	for _, queue := range queues {
		tasks, err := g.inspector.ListActiveTasks(queue)
		if err != nil {
			return false
		}
		l := len(tasks)
		for i := 0; i < l; i++ {
			if tasks[i].Type == name {
				currentTasks = append(currentTasks, tasks[i])
			}
		}
	}
	m := len(currentTasks)
	if m > 1 {
		return true
	}
	return false
}
