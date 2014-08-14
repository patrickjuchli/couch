package couch

import "fmt"

// TODO What if this application stops, restarts - CouchDB will still be running replications. Add db.Replications() []Replication
// TODO add: list of running replications
// TODO add: replication.Running()
// TODO add: replication filter
// TODO what about permissions? if target has different login?

// A replication from a source to a target
type Replication struct {
	source     *Database
	target     *Database
	continuous bool
}

// A bidirectional replication
type Sync struct {
	replA2B *Replication
	replB2A *Replication
}

type replRequest struct {
	CreateTarget bool   `json:"create_target"`
	Source       string `json:"source"`
	Target       string `json:"target"`
	Continuous   bool   `json:"continuous"`
	Cancel       bool   `json:"cancel",omitempty`
}

// Replicates given database to a target database. If the target database
// does not exist it will be created. The target database may be on a different host.
func (db *Database) ReplicateTo(target *Database, continuously bool) (*Replication, error) {
	req := replRequest{CreateTarget: true, Source: db.Url(), Target: target.UrlWithCredentials(), Continuous: continuously}
	resp, err := request("POST", db.replicationUrl(), req, nil)
	if err != nil {
		return nil, err
	}
	var repl *Replication
	switch resp.StatusCode {
	case 200, 202:
		repl = &Replication{source: db, target: target, continuous: continuously}
	case 400:
		err = ErrBadRequest
	case 401:
		err = ErrNoAdmin
	case 404:
		err = ErrDatabaseUnknown
	case 500:
		err = ErrInvalidJsonRequest
	}
	return repl, err
}

// Cancel a continuously running replication
func (repl *Replication) Cancel() error {
	req := replRequest{CreateTarget: true, Source: repl.source.Url(), Target: repl.target.Url(), Continuous: repl.continuous, Cancel: true}
	resp, err := request("POST", repl.source.replicationUrl(), req, nil)
	if err != nil {
		return err
	}
	switch resp.StatusCode {
	case 400:
		err = ErrBadRequest
	case 401:
		err = ErrNoAdmin
	case 404:
		err = ErrDatabaseUnknown
	case 500:
		err = ErrInvalidJsonRequest
	}
	return err
}

// Returns replication source
func (repl *Replication) Source() *Database {
	return repl.source
}

// Returns replication target
func (repl *Replication) Target() *Database {
	return repl.target
}

// Returns whether replication is running continuously or not
func (repl *Replication) Continuous() bool {
	return repl.continuous
}

// Synchronizes two databases by setting up two replications, one from given database
// to target and from target to given database. If the target database does not exist it will be created.
// The target database may be on a different host.
//
// This method may be convenient but note that it is not atomic: Sync means that this method will first
// replicate db to target and then target to db. If the first one fails, both fail. If the first one works but
// the second doesn't, the first one will have executed nonetheless. If the sync has been set up to be continuous,
// the first continuous replication will be cancelled if the second one fails.
func (db *Database) SyncWith(target *Database, continuously bool) (*Sync, error) {
	replA2B, err := db.ReplicateTo(target, continuously)
	if err != nil {
		return nil, err
	}
	replB2A, err := target.ReplicateTo(db, continuously)
	if err != nil {
		replA2B.Cancel()
		return nil, err
	}
	sync := &Sync{replA2B, replB2A}
	return sync, nil
}

// Cancel a continuously running sync
func (sync *Sync) Cancel() error {
	// Call cancel directly on both replications, if replA2B.Cancel() leads to
	// an error we might still be able to cancel replB2A. Errors will be combined later.
	errA2B := sync.replA2B.Cancel()
	errB2A := sync.replB2A.Cancel()
	var err error
	if errA2B != nil || errB2A != nil {
		err = fmt.Errorf("couch: Error cancelling replication, a->b: %v, b->a: %v", errA2B, errB2A)
	}
	return err
}

// // Returns forward replication
// func (sync *Sync) ReplAToB() *Replication {
// 	return sync.replA2B
// }

// // Returns backward replication
// func (sync *Sync) ReplBToA() *Replication {
// 	return sync.replB2A
// }

func (db *Database) replicationUrl() string {
	return db.server.url + "/_replicate"
}
