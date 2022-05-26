package sqlite

import (
	"database/sql"
	"log"
	"sync"
)

// Checkpointer is an SQLite WAL checkpointer, it is workaround before WAL2 becomes common:
// https://www.sqlite.org/cgi/src/doc/wal2/doc/wal2.md
// Currently, concurrent writes in SQLite make the WAL file grow without limit
type Checkpointer struct {
	m     sync.Mutex
	wg    sync.WaitGroup
	DB    *sql.DB
	Limit int
	i     int
}

// Checkpoint checks if the number of times it has been called reaches the limit
// If it did, it blocks until all other functions that call it are finished and performs a checkpoint
// It is intended to be used like this:
//
// 	var c = sqlite.NewCheckPointer(1000, db)
// 	func() {
// 		defer c.Checkpoint()()
// 		db.Exec(`insert into "table" values ("value")`)
// 	}
func (c *Checkpointer) Checkpoint() func() {
	c.m.Lock()
	c.i++
	if c.i == c.Limit {
		c.i = 0
		c.wg.Wait()
		var failed bool
		if err := c.DB.QueryRow(`pragma wal_checkpoint(restart)`).Scan(&failed, new(int), new(int)); err != nil {
			log.Println("checkpointing:", err)
		} else if failed {
			log.Println("checkpointing failed")
		}
	}
	c.wg.Add(1)
	c.m.Unlock()
	return c.wg.Done
}
