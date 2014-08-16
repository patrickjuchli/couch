package couch

import (
	"fmt"
	"net/url"
)

// TODO add: db.RunningReplications() []Replication
// TODO add: repl.Running()

// A replication from a source to a target
type Replication struct {
	source     *Database
	target     *Database
	continuous bool
	sessionID  string
}

// A bidirectional replication
type Sync struct {
	replA2B *Replication
	replB2A *Replication
}

// CouchDB request for replication
type replRequest struct {
	CreateTarget bool   `json:"create_target"`
	Source       string `json:"source"`
	Target       string `json:"target"`
	Continuous   bool   `json:"continuous"`
	Cancel       bool   `json:"cancel,omitempty"`
}

// CouchDB response to replication request
type replResponse struct {
	Ok            bool   `json:"ok"`
	ReplIDVersion int    `json:"replication_id_version"`
	SessionID     string `json:"session_id"`
	SourceLastSeq int    `json:"source_last_seq"`
}

// Replicates given database to a target database. If the target database
// does not exist it will be created. The target database may be on a different host.
func (db *Database) ReplicateTo(target *Database, continuously bool) (*Replication, error) {
	var resp replResponse
	req := replRequest{CreateTarget: true, Source: db.URL(), Target: target.urlWithCredentials(), Continuous: continuously}
	_, err := Do(db.replicationURL(), "POST", db.Cred(), req, &resp)
	if err != nil {
		return nil, err
	}
	repl := &Replication{source: db, target: target, continuous: continuously, sessionID: resp.SessionID}
	return repl, err
}

// Cancel a continuously running replication
func (repl *Replication) Cancel() error {
	req := replRequest{CreateTarget: true, Source: repl.source.URL(), Target: repl.target.URL(), Continuous: repl.continuous, Cancel: true}
	_, err := Do(repl.source.replicationURL(), "POST", repl.source.Cred(), req, nil)
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

func (repl *Replication) SessionID() string {
	return repl.sessionID
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

// Not safe, only used body of replication request
func (db *Database) urlWithCredentials() string {
	result, _ := url.Parse(db.URL())
	result.User = url.UserPassword(db.server.cred.user, db.server.cred.password)
	return result.String()
}

func (db *Database) replicationURL() string {
	return db.server.url + "/_replicate"
}
