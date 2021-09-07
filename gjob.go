package gjob

import (
	"context"
	"fmt"
	"github.com/hibiken/asynq"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	RedisUri string
}

type GoodJob struct {
	lock         sync.Mutex
	started      bool
	server       *asynq.Server
	scheduler    *asynq.Scheduler
	mux          *asynq.ServeMux
	inspector    *asynq.Inspector
	redis        asynq.RedisConnOpt
	active       map[string]*int32
	tasks        map[string]GoodTask
	queueTasks   map[string]GoodTask
	handles      map[string]func(context.Context, GoodTask) error
	queueHandles map[string]func(context.Context, GoodTask) error
	entries      map[string]string
}

type GoodTask struct {
	Id   string
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

	// init fields
	job := GoodJob{
		redis: opt,
	}
	job.active = make(map[string]*int32)
	job.tasks = make(map[string]GoodTask)
	job.queueTasks = make(map[string]GoodTask)
	job.handles = make(map[string]func(context.Context, GoodTask) error)
	job.queueHandles = make(map[string]func(context.Context, GoodTask) error)
	job.entries = make(map[string]string)
	job.inspector = asynq.NewInspector(job.redis)
	job.server = asynq.NewServer(
		job.redis,
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
	job.mux = asynq.NewServeMux()
	job.scheduler = asynq.NewScheduler(
		job.redis,
		&asynq.SchedulerOpts{
			Logger:              nil,
			LogLevel:            asynq.DebugLevel,
			Location:            time.Local,
			EnqueueErrorHandler: nil,
		},
	)
	return &job, nil
}

func (g *GoodJob) AddJob(taskName, expr, taskJson string, handler func(context.Context, GoodTask) error) *GoodJob {
	g.lock.Lock()
	defer g.lock.Unlock()
	task := asynq.NewTask(taskName, []byte(taskJson))
	g.tasks[taskName] = GoodTask{
		Name: task.Type(),
		Expr: expr,
		Json: string(task.Payload()),
		t:    task,
	}
	g.handles[taskName] = handler
	return g
}

func (g *GoodJob) Start() error {
	g.lock.Lock()
	defer g.lock.Unlock()
	if len(g.tasks) == 0 {
		return fmt.Errorf("please add job before start")
	}
	if len(g.handles) == 0 {
		return fmt.Errorf("please add job before start")
	}
	if !g.started {
		// the server only needs to be started once
		g.mux = asynq.NewServeMux()
		go g.scheduler.Run()
		go g.server.Run(g.mux)
		g.started = true
	}

	for name, handle := range g.handles {
		for taskName, task := range g.tasks {
			if taskName == name {
				// register task
				entryId, err := g.scheduler.Register(
					task.Expr,
					task.t,
					asynq.MaxRetry(0),
					asynq.Timeout(24*time.Hour),
				)
				if err != nil {
					return err
				}
				g.entries[name] = entryId
				// register task callback function
				fun := (func(job *GoodJob, id, expr string, f func(ctx context.Context, t GoodTask) error) func(ctx context.Context, t *asynq.Task) error {
					return func(ctx context.Context, t *asynq.Task) error {
						active := job.jobIsActive(t.Type())
						if active {
							return nil
						}
						task := GoodTask{
							Id:   entryId,
							Expr: expr,
							Name: t.Type(),
							Json: string(t.Payload()),
							t:    t,
						}
						return f(ctx, task)
					}
				})(g, entryId, task.Expr, handle)
				g.mux.HandleFunc(name, fun)
			}
		}
	}

	for name := range g.tasks {
		g.queueTasks[name] = g.tasks[name]
		i := int32(0)
		g.active[name] = &i
	}
	for name := range g.handles {
		g.queueHandles[name] = g.handles[name]
	}
	g.tasks = make(map[string]GoodTask)
	g.handles = make(map[string]func(context.Context, GoodTask) error)
	return nil
}

func (g *GoodJob) Stop(taskName string) error {
	g.lock.Lock()
	defer g.lock.Unlock()
	entry, ok := g.entries[taskName]
	if !ok {
		return fmt.Errorf("job %s not register", taskName)
	}
	err := g.scheduler.Unregister(entry)
	if err != nil {
		return err
	}
	delete(g.active, taskName)
	delete(g.queueTasks, taskName)
	delete(g.queueHandles, taskName)
	return nil
}

// check job status is active(the same task name)
func (g *GoodJob) jobIsActive(name string) bool {
	g.lock.Lock()
	defer g.lock.Unlock()
	active, ok := g.active[name]
	if !ok {
		return true
	}
	v := atomic.LoadInt32(active)
	if v == 1 {
		return true
	}
	atomic.AddInt32(active, 1)
	defer atomic.AddInt32(active, -1)
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
