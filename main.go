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
	s := "host=localhost port=6432 user=postgres password=secret dbname=postgres pool_max_conns=10 default_query_exec_mode=simple_protocol"
	c, err := pgxpool.ParseConfig(s)
	if err != nil {
		panic("failed to parse postgres config: " + err.Error())
	}

	c.ConnConfig.BuildContextWatcherHandler = func(pgConn *pgconn.PgConn) ctxwatch.Handler {
		// return &pgconn.DeadlineContextWatcherHandler{Conn: pgConn.Conn()}
		return &SyncCancelRequestContextWatcherHandler{PgConn: pgConn}
	}

	return pgxpool.NewWithConfig(ctx, c)
}

type SyncCancelRequestContextWatcherHandler struct {
	PgConn *pgconn.PgConn
	// Logger *zap.Logger
}

//nolint:contextcheck
func (h *SyncCancelRequestContextWatcherHandler) HandleCancel(context.Context) {
	// h.PgConn.Conn().Close()

	h.PgConn.Conn().SetDeadline(time.Now().Add(1 * time.Second))

	// select {
	// // case <-h.PgConn.CleanupDone():
	// // h.Logger.Info("HandleCancel: CleanupDone")
	// case <-ctx.Done():
	// 	// h.Logger.Info("HandleCancel: CtxDone")
	// }
}

func (h *SyncCancelRequestContextWatcherHandler) HandleUnwatchAfterCancel() {
	h.PgConn.Conn().SetDeadline(time.Time{})

	// h.PgConn.Conn().SetDeadline(time.Time{})

	// <-h.PgConn.CleanupDone()

	// ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second) //nolint:gomnd
	// defer cancel()

	// h.PgConn.Close(context.Background())

	// h.PgConn.Conn().Close()
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
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	q := `select pg_sleep(10)`
	rows, err := db.Query(ctx, q)
	fmt.Println(err)
	defer rows.Close()
}
