package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/pgxpool"
)

func NewDatabasePool(ctx context.Context) (*pgxpool.Pool, error) {
	s := "host=pgbouncer port=6432 user=postgres password=secret dbname=postgres pool_max_conns=10 default_query_exec_mode=simple_protocol"
	c, err := pgxpool.ParseConfig(s)
	if err != nil {
		panic("failed to parse postgres config: " + err.Error())
	}

	c.ConnConfig.BuildContextWatcherHandler = func(pgConn *pgconn.PgConn) ctxwatch.Handler {
		// return &pgconn.DeadlineContextWatcherHandler{Conn: pgConn.Conn()}
		return &SyncCancelRequestContextWatcherHandler{Conn: pgConn}
	}

	return pgxpool.NewWithConfig(ctx, c)
}

type SyncCancelRequestContextWatcherHandler struct {
	Conn *pgconn.PgConn

	// CancelRequestDelay is the delay before sending the cancel request to the server.
	CancelRequestDelay time.Duration

	// DeadlineDelay is the delay to set on the deadline set on net.Conn when the context is canceled.
	DeadlineDelay time.Duration

	cancelFinishedChan             chan struct{}
	handleUnwatchAfterCancelCalled func()
}

func (h *SyncCancelRequestContextWatcherHandler) HandleCancel(context.Context) {
	h.cancelFinishedChan = make(chan struct{})
	defer close(h.cancelFinishedChan)

	var handleUnwatchedAfterCancelCalledCtx context.Context
	handleUnwatchedAfterCancelCalledCtx, h.handleUnwatchAfterCancelCalled = context.WithCancel(context.Background())

	deadline := time.Now().Add(h.DeadlineDelay)
	h.Conn.Conn().SetDeadline(deadline)

	select {
	case <-handleUnwatchedAfterCancelCalledCtx.Done():
		return
	case <-time.After(h.CancelRequestDelay):
	}

	cancelRequestCtx, cancel := context.WithDeadline(handleUnwatchedAfterCancelCalledCtx, deadline)
	defer cancel()
	h.Conn.CancelRequest(cancelRequestCtx)
}

func (h *SyncCancelRequestContextWatcherHandler) HandleUnwatchAfterCancel() {
	h.handleUnwatchAfterCancelCalled()
	<-h.cancelFinishedChan

	h.Conn.Conn().SetDeadline(time.Time{})
}

func main() {
	for {
		err := run()
		fmt.Println("run() finished: ", err)
		time.Sleep(10 * time.Second)
	}
}

func run() error {
	ctx := context.Background()

	db, err := NewDatabasePool(ctx)
	if err != nil {
		return err
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go worker(ctx, db, wg)
	}
	wg.Wait()

	return nil
}

func worker(ctx context.Context, db *pgxpool.Pool, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		handle(ctx, db)
	}
}

func handle(ctx context.Context, db *pgxpool.Pool) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()

	q := `select pg_sleep(10)`
	rows, err := db.Query(ctx, q)
	fmt.Println(err)
	defer rows.Close()
}
