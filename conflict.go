package couch

import (
	"encoding/json"
	"errors"
)

var (
	// Design document for conflicts view
	ConflictsDesignID = "conflicts"

	// Name of the view to query documents with conflicts
	ConflictsViewID = "all"
)

// Describes a conflict between different document revisions.
// Opaque type, use associated methods.
type Conflict struct {
	db        *Database
	docID     string
	revisions []DynamicDoc
}

// Get conflicting revisions for a document id. Returns nil if there are no conflicts.
func (db *Database) ConflictFor(docID string) (*Conflict, error) {
	revs, err := db.openRevsFor(docID)
	if err != nil {
		return nil, err
	}
	openLeaves := filterOpenLeafDocs(revs)
	if len(openLeaves) == 1 { // One alone does not a conflict make
		return nil, nil
	}
	return &Conflict{revisions: openLeaves, docID: docID, db: db}, nil
}

// Solves a conflict with a final document. It will set the revision id of
// the document to the final revision id that CouchDB will report once the operation is complete.
//
// If the operation is successful, the conflict c will no longer hold any information about
// the formerly conflicting revisions.
//
// Be aware that while you solve a conflict, another party might have done so right before
// you. In this case of a lost update you will receive an error. You should
// then ask about the state of the conflict again using db.ConflictFor(myDocID).
func (c *Conflict) SolveWith(finalDoc Identifiable) error {
	if !c.isReal() {
		return nil
	}

	// Make finalDoc the new leaf of the first open branch.
	// To do so, assign it the revision id of the first revision.
	id, rev := c.revisions[0].IDRev()
	finalDoc.SetIDRev(id, rev)
	leaves := new(Bulk)
	leaves.Add(finalDoc)

	// Close all other open branches by marking their leaves deleted
	for _, rev := range c.revisions[1:] {
		rev["_deleted"] = true
		leaves.Add(rev)
	}
	_, err := c.db.InsertBulk(leaves, true)
	if err == nil {
		c.revisions = nil
	}
	return err
}

// Get all conflicting document revisions in a preferred format.
// It supports the same types for v as json.Unmarshal.
//
// Revisions in a slice of structs:
//  var revs []MyStruct
//  conflict.Revisions(&revs)
//
// Other examples:
//  var revs interface{}
//  var revs []map[string]interface{}
//
// Note that map[string]interface{} will not work.
func (c *Conflict) Revisions(v interface{}) {
	// Converting []map[string]interface{} to a type provided by the user.
	// Using Marshal/Unmarshal is not exactly a great solution but still
	// faster and less memory intensive than e.g. the mapstructure package.
	// Alternative?
	tmp, _ := json.Marshal(c.revisions)
	json.Unmarshal(tmp, v)
}

// IsReal checks if there are really conflicting revisions to solve.
func (c *Conflict) isReal() bool {
	return c.revisions != nil && len(c.revisions) > 1
}

// Returns all conflicts in a database. To do so, a dedicated view is necessary at
// [db-url]/_design/conflicts/_view/all. If it doesn't exist and forceView is enabled,
// it will be automatically set up.
//
// Note, that if the database is already large at that point, this operation can take
// a very long time. It's recommended to call this method or ConflictsCount() right after
// creating a new database.
func (db *Database) Conflicts(forceView bool) (docIDs []string, err error) {
	result, err := db.queryConflictView(forceView, false)
	if err != nil {
		return
	}
	n := len(result.Rows)
	docIDs = make([]string, n)
	for i, row := range result.Rows {
		docIDs[i] = row.ID
	}
	return
}

// Returns the number of conflicts, sets up view if forceView is enabled.
// See db.Conflicts() for possible issues around creating a view.
func (db *Database) ConflictsCount(forceView bool) (int, error) {
	result, err := db.queryConflictView(forceView, true)
	if err != nil {
		return 0, err
	}
	if len(result.Rows) > 0 {
		return result.Rows[0].ValueInt(), nil
	}
	return 0, nil
}

func (db *Database) queryConflictView(forceView bool, reduce bool) (*ViewResult, error) {
	options := map[string]interface{}{
		"reduce": reduce,
	}
	err := db.ensureConflictView(forceView)
	if err != nil {
		return nil, err
	}
	result, err := db.Query(ConflictsDesignID, ConflictsViewID, options)
	return result, err
}

// Make sure a conflict view exist, if not, create it if forceView is enabled
func (db *Database) ensureConflictView(forceView bool) error {
	if db.HasView(ConflictsDesignID, ConflictsViewID) {
		return nil
	}
	if !forceView {
		return errors.New("missing conflicts view")
	}
	err := db.createConflictView()
	return err
}

// Inserts a design document with a view containting a map function to collect
// document ids with conflicts and a reduce function to count them.
func (db *Database) createConflictView() error {
	view := view{}
	view.Map = `function(doc) { if (doc._conflicts) { emit(null, null); } }`
	view.Reduce = `_count`
	design := newDesign()
	design.Views["all"] = view
	design.SetIDRev("_design/"+ConflictsDesignID, "")
	err := db.Insert(design)
	return err
}

// Used to read out CouchDBs answer to open_revs and filter by 'ok' field (=available revision)
// See http://docs.couchdb.org/en/latest/replication/conflicts.html#working-with-conflicting-documents
type openRevision struct {
	Doc DynamicDoc `json:"ok"`
}

// Gets all open and available revisions of a document (including _deleted ones)
func (db *Database) openRevsFor(docID string) ([]openRevision, error) {
	params := map[string]interface{}{"open_revs": "all"}
	var revs []openRevision
	err := db.retrieve(docID, "", &revs, params)
	return revs, err
}

// Returns docs that are not marked as deleted
func filterOpenLeafDocs(revs []openRevision) []DynamicDoc {
	var openRevs []DynamicDoc
	for _, rev := range revs {
		del, ok := rev.Doc["_deleted"]
		if !ok || del == false {
			openRevs = append(openRevs, rev.Doc)
		}
	}
	return openRevs
}
