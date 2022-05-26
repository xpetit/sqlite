package sqlite

import (
	"database/sql"
	"log"
	"sync"
)

// Checkpointer is an SQLite WAL checkpointer, it is workaround before WAL2 becomes common:
// https://www.sqlite.org/cgi/src/doc/wal2/doc/wal2.md
// Currently, concurrent writes in SQLite make the WAL file grow without limit
type Checkpointer interface {
	// Checkpoint checks if the number of times it has been called reaches the limit
	// If it did, it blocks until all other functions that call it are finished and performs a checkpoint
	// It is intended to be used like this:
	//
	// 	var c = sqlite.NewCheckPointer(db, 1000)
	// 	func() {
	// 		defer c.Checkpoint()()
	// 		db.Exec(`insert into "table" values ("value")`)
	// 	}
	Checkpoint() func()
}

var _ Checkpointer = (*checkpointer)(nil)

type checkpointer struct {
	m     sync.Mutex
	wg    sync.WaitGroup
	db    *sql.DB
	limit uint
	i     uint
}

func NewCheckPointer(db *sql.DB, limit uint) (Checkpointer, error) {
	if _, err := db.Exec(`pragma wal_autocheckpoint = 0`); err != nil {
		return nil, err
	}

	return &checkpointer{
		db:    db,
		limit: limit,
	}, nil
}

func (c *checkpointer) Checkpoint() func() {
	c.m.Lock()
	if c.i < c.limit {
		c.i++
	} else {
		c.i = 0
		c.wg.Wait()
		var failed bool
		if err := c.db.QueryRow(`pragma wal_checkpoint(restart)`).Scan(&failed, new(uint), new(uint)); err != nil {
			log.Println("checkpointing:", err)
		} else if failed {
			log.Println("checkpointing failed")
		}
	}
	c.wg.Add(1)
	c.m.Unlock()
	return c.wg.Done
}
