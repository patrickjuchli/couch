package couch

import (
	"encoding/json"
	"testing"
)

// Most tests are integration tests that need a running CouchDB
var (
	testHost              = "http://localhost:5984"
	testDB                = "couch_test_go"
	testCred *Credentials = nil //NewCredentials("much", "safe")
)

// Use testing flags!

// TODO When setUpDatabase use different database each time (via counter) to make
// integration tests more isolated and allow for parallel execution
// TODO test when adding two docs to a bulk with same id but differing revisions

// Just an arbitrary test struct
type person struct {
	Doc
	Name   string
	Height uint8
	Alive  bool
}

func TestDocJson(t *testing.T) {
	t.Parallel()
	ref := Doc{}
	enc, _ := json.Marshal(ref)
	dec := make(map[string]interface{})
	json.Unmarshal(enc, dec)
	if len(dec) > 0 {
		t.Error("Json-encoded Doc with empty ID and Rev, resulting Json should omit both fields but it doesn't:", dec)
	}
}

func TestIdentifiableDoc(t *testing.T) {
	t.Parallel()
	doc := person{Name: "Peter", Height: 185}
	id, rev := doc.IDRev()
	if id != "" || rev != "" {
		t.Fatal("ID and rev should be empty but aren't:", id, rev)
	}
	doc.SetIDRev("foo", "bar")
	id, rev = doc.IDRev()
	if id != "foo" || rev != "bar" {
		t.Fatal("ID and rev should be 'foo' and 'bar', but aren't:", id, rev)
	}
}

func TestIdentifiableDynamicDoc(t *testing.T) {
	t.Parallel()
	doc := DynamicDoc{"Name": "Peter"}
	id, rev := doc.IDRev()
	if id != "" || rev != "" {
		t.Fatal("ID and rev should be empty but aren't:", id, rev)
	}
	doc.SetIDRev("foo", "bar")
	id, rev = doc.IDRev()
	if id != "foo" || rev != "bar" {
		t.Fatal("ID and rev should be 'foo' and 'bar', but aren't:", id, rev)
	}
}

func TestBulk(t *testing.T) {
	t.Parallel()

	bulk := new(DocBulk)
	bulk.Add(&person{Name: "Peter", Height: 160})
	bulk.Add(&person{Name: "Anna", Height: 170})

	bulk.Docs[0].SetIDRev("1", "A")
	bulk.Docs[1].SetIDRev("2", "B")

	doc := bulk.Find("2", "C")
	if doc != nil {
		t.Error("Looking for non-existing doc in bulk, found something", doc)
	}
	doc = bulk.Find("2", "B")
	if doc == nil {
		t.Error("Looking for existing doc in bulk, not found")
	}
}

func TestDatabase(t *testing.T) {
	t.Parallel()
	db := server().Database("foo")
	if db.Name() != "foo" {
		t.Error("Name of database reported incorrectly, should be foo, is", db.Name())
	}
}

func TestIntegrationDBExists(t *testing.T) {
	db := setUpDatabase(t)
	if !db.Exists() {
		t.Error("Created database", db.Name(), "db.Exist() should return true")
	}
	db.DropDatabase()
	if db.Exists() {
		t.Error("Deleted database", db.Name(), "db.Exist() should return false")
	}
}

func TestIntegrationInsert(t *testing.T) {
	db := setUpDatabase(t)
	defer tearDownDatabase(db, t)

	// Add new document
	doc := &person{Name: "Peter", Height: 185, Alive: true}
	err := db.Insert(doc)
	if err != nil {
		t.Fatal("Inserted new document, error:", err)
	}
	if doc.ID == "" {
		t.Error("Inserted new document, should have ID set. Doc:", doc)
	}
	if doc.Rev == "" {
		t.Error("Inserted new document, should have Rev set. Doc:", doc)
	}

	if t.Failed() {
		t.FailNow()
	}

	// Edit existing
	oldID, oldRev := doc.ID, doc.Rev
	doc.Alive = false
	err = db.Insert(doc)
	if doc.Rev == oldRev {
		t.Error("Edited existing document, should have different rev. Doc:", doc)
	}
	if doc.ID != oldID {
		t.Error("Edited existing document, should have same id. Old:", oldID, "New:", doc.ID, doc)
	}
	if err != nil {
		t.Fatal("Edited existing document, error:", err)
	}
}

func TestIntegrationBulkInsert(t *testing.T) {
	db := setUpDatabase(t)
	defer tearDownDatabase(db, t)

	bulk := new(DocBulk)
	bulk.Add(&person{Name: "Peter", Height: 160})
	bulk.Add(&person{Name: "Anna", Height: 170})
	bulk.Add(&person{Name: "Stefan", Height: 180})

	failedBulk, err := db.InsertBulk(bulk, true)
	if err != nil {
		t.Fatal("Inserting bulk of documents returns error:", err, "failed docs:", failedBulk.Docs)
	}

	for _, doc := range bulk.Docs {
		id, rev := doc.IDRev()
		if id == "" || rev == "" {
			t.Error("Newly added document in bulk should have id and rev set after opereration but doesn't:", doc)
		}
	}
}

func TestIntegrationRetrieve(t *testing.T) {
	db := setUpDatabase(t)
	defer tearDownDatabase(db, t)

	// Add new document (see TestIntegrationInsert)
	original := &person{Name: "Peter", Height: 185, Alive: true}
	insertTestDoc(original, db, t)

	// Retrieve it
	retrieved := new(person)
	err := db.Retrieve(original.ID, retrieved)
	if err != nil {
		t.Error("Retrieving newly created document returns error:", err)
	}
	if retrieved.ID != original.ID {
		t.Error("Retrieved document, has not same ID when added. Original:", original, "Retrieved:", retrieved)
	}
	if retrieved.Name != original.Name || retrieved.Height != original.Height || retrieved.Alive != original.Alive {
		t.Error("Retrieved document, has not same data when added. Original:", original, "Retrieved:", retrieved)
	}
}

func TestIntegrationLostUpdate(t *testing.T) {
	db := setUpDatabase(t)
	defer tearDownDatabase(db, t)

	// 1. Insert new document
	original := &person{Name: "Peter", Height: 185, Alive: true}
	insertTestDoc(original, db, t)

	// 2. Retrieve document twice, Doc1, Doc2
	doc1 := new(person)
	err := db.Retrieve(original.ID, doc1)
	if err != nil {
		t.Fatal("Retrieving newly created document returns error:", err)
	}

	// Fake second retrieve through copy
	docCopy := *doc1
	doc2 := &docCopy

	// 3. Change both Doc1 and Doc2 independently
	doc1.Name = "Peter Doc1"
	doc2.Name = "Peter Doc2"

	// 4. Insert Doc1
	err = db.Insert(doc1)
	if err != nil {
		t.Fatal("Edited existing document (doc1), error:", err)
	}

	// 5. Insert Doc2 (using same revision as Doc1 which is now invalid), this should provoke a conflict
	err = db.Insert(doc2)
	if err == nil || ErrorType(err) != "conflict" {
		t.Error("Inserted document with old revision, should provoke conflict but didn't, error:", err)
	}
}

func TestIntegrationReplicate(t *testing.T) {
	db := setUpDatabase(t)
	defer tearDownDatabase(db, t)

	doc := &person{Name: "Peter", Height: 185, Alive: true}
	insertTestDoc(doc, db, t)

	// Replicate
	targetDb := server().Database("repl_test")
	repl, err := db.ReplicateTo(targetDb, false)
	if err != nil {
		t.Fatal("Replication returned error:", err)
	}
	if repl == nil {
		t.Error("No replication handle returned")
	}
	if repl.Source() == nil {
		t.Error("Replication handle doesn't contain source database")
	}
	if repl.Target() == nil {
		t.Error("Replication handle doesn't contain target database")
	}
	if repl.Continuous() == true {
		t.Error("Replication handle says replication is continuous but it isn't")
	}
	if repl.SessionID() == "" {
		t.Error("Missing sessionID for replication")
	}

	// Retrieve doc from replicated db and compare
	replDoc := new(person)
	err = targetDb.Retrieve(doc.ID, replDoc)
	if err != nil {
		t.Error("Retrieving doc from replicated database failed with error:", err)
	}
	if replDoc.Name != doc.Name || replDoc.Height != doc.Height {
		t.Error("Original and replicated document are not equal. Original:", doc, "Replication:", replDoc)
	}

	targetDb.DropDatabase()
}

func TestIntegrationNoConflict(t *testing.T) {
	db := setUpDatabase(t)
	defer tearDownDatabase(db, t)

	// Insert a document
	doc := &person{Name: "Original", Height: 185, Alive: true}
	insertTestDoc(doc, db, t)

	// Check for conflict
	conflict, err := db.ConflictFor(doc.ID)
	if err != nil {
		t.Fatal("Couldn't get conflicts for document", doc.ID, ", error:", err)
	}
	if conflict != nil {
		numConflictingRevs := conflict.RevisionsCount()
		if numConflictingRevs > 0 {
			t.Fatal("Expected no conflicts for a document but got", numConflictingRevs)
		}
	}
}

// Full replication cycle leading to a conflict, executing and checking on conflict resolution
func TestIntegrationReplicateWithConflict(t *testing.T) {
	db := setUpDatabase(t)
	defer tearDownDatabase(db, t)

	// Insert doc
	originDoc := &person{Name: "Original", Height: 185, Alive: true}
	insertTestDoc(originDoc, db, t)

	// Replicate db to target
	targetDb := server().Database("repl_test")
	targetDb.DropDatabase()
	defer targetDb.DropDatabase()
	_, err := db.ReplicateTo(targetDb, false)
	if err != nil {
		t.Fatal("Replication origin->target returned error:", err)
	}

	// Edit the doc on origin
	originDoc.Name = "Edit on origin"
	insertTestDoc(originDoc, db, t)

	// Edit the doc on target
	targetDoc := new(person)
	targetDb.Retrieve(originDoc.ID, targetDoc)
	targetDoc.Name = "Edit on target"
	insertTestDoc(targetDoc, targetDb, t)

	// Replicate from target back to origin
	_, err = targetDb.ReplicateTo(db, false)
	if err != nil {
		t.Fatal("Replication target->origin returned error:", err)
	}

	// Doc on origin should now have 2 conflicting revisions
	conflict, err := db.ConflictFor(originDoc.ID)
	if err != nil {
		t.Fatal("Couldn't get conflicts for document", originDoc.ID, ", error:", err)
	}
	if conflict == nil {
		t.Fatal("Expected 2 conflicting revisions, but got none at all")
	}
	numConflictingRevs := conflict.RevisionsCount()
	if numConflictingRevs != 2 {
		t.Fatal("Expected 2 conflicting revisions, but got", numConflictingRevs)
	}

	// It's useful to have conflicting revisions accesible with a struct if possible
	var revs []person
	conflict.Revisions(&revs)
	if len(revs) != 2 {
		t.Error("There should be two conflicting revisions represented by struct person but got", len(revs))
	}
	nameA, nameB := revs[0].Name, revs[1].Name
	if !((nameA == "Edit on origin" && nameB == "Edit on target") || (nameA == "Edit on target" && nameB == "Edit on origin")) {
		t.Error("Content of conflicting revisions has not been correctly presented, got", revs)
	}

	// Intermezzo: See if general conflict detection works too
	ids, err := db.Conflicts(true)
	if err != nil {
		t.Fatal("Get all conflicting documents retured error:", err)
	}
	if len(ids) != 1 {
		t.Fatal("Get all conflicting documents, didn't find exactly 1 but:", len(ids))
	}
	if ids[0] != originDoc.ID {
		t.Fatal("When using db.Conflicts(), the mentioned id is not the same as expected but:", ids[0])
	}

	// Intermezzo: Number of all conflicts in a database
	numConflicts, err := db.ConflictsCount(true)
	if err != nil {
		t.Fatal("Get number of conflicting documents retured error:", err)
	}
	if numConflicts != 1 {
		t.Fatal("Number of conflicting documents is not 1 but", numConflicts)
	}

	// Solve conflict
	solution := &person{Name: "Solution", Height: 185, Alive: true}
	err = conflict.SolveWith(solution)
	if err != nil {
		t.Fatal("Solving the conflict produced error:", err)
	}

	// Try solving again
	err = conflict.SolveWith(solution)
	if err != nil {
		t.Fatal("Solving solved conflict again returned error", err)
	}

	// Does solution have the correct id and a new revision id?
	if solution.ID != originDoc.ID {
		t.Fatal("Solution doc was not assigned same id of the conflicted doc:", solution.ID, "original doc:", originDoc.ID)
	}
	if solution.Rev == "" {
		t.Fatal("Solution doc was not assigned a revision id")
	}

	// Check again if really solved
	conflict, err = db.ConflictFor(originDoc.ID)
	if err != nil {
		t.Fatal("Couldn't get conflicts for document", originDoc.ID, ", error:", err)
	}
	if conflict != nil {
		t.Fatal("Conflict should be solved, still conflicting revisions for doc:", conflict)
	}

	// Check again overall db
	numConflicts, err = db.ConflictsCount(true)
	if err != nil {
		t.Fatal("Get number of conflicting documents retured error:", err)
	}
	if numConflicts != 0 {
		t.Fatal("Number of conflicting documents should be 0, is", numConflicts)
	}

}

func TestIntegrationConflictView(t *testing.T) {
	db := setUpDatabase(t)
	defer tearDownDatabase(db, t)

	if db.HasView(conflictsDesignID, conflictsViewID) {
		t.Fatal("Shouldn't have conflict view on newly created db but reports that it has")
	}
	err := db.createConflictView()
	if err != nil {
		t.Fatal("Created conflict view, got error:", err)
	}
	if !db.HasView(conflictsDesignID, conflictsViewID) {
		t.Fatal("Should have conflict view but reports that it hasn't")
	}
}

func TestIntegrationDelete(t *testing.T) {
	db := setUpDatabase(t)
	defer tearDownDatabase(db, t)

	// Insert doc
	originDoc := &person{Name: "Original", Height: 185, Alive: true}
	insertTestDoc(originDoc, db, t)

	// Delete doc
	err := db.Delete(originDoc.ID, originDoc.Rev)
	if err != nil {
		t.Fatal("Deleting a document returned error:", err)
	}

	// Try to retrieve doc
	doc := new(person)
	err = db.Retrieve(originDoc.ID, doc)
	if err == nil {
		t.Fatal("Retrieving deleted document did not return error but doc:", doc)
	}
}

func TestDo(t *testing.T) {
	db := setUpDatabase(t)
	defer tearDownDatabase(db, t)

	localCred := testCred

	// Wrong host
	_, err := Do("http://127.0.0.1:598/couch_test_go/_compact", "POST", localCred, nil, nil)
	if err == nil {
		t.Fatal("Wrong host should return error")
	}

	// Wrong db name
	_, err = Do("http://127.0.0.1:5984/couch_WRONG_go/_compact", "POST", localCred, nil, nil)
	if err == nil {
		t.Fatal("Wrong db name should return error")
	}

	// Wrong API
	_, err = Do("http://127.0.0.1:5984/couch_test_go/_coooompact", "POST", localCred, nil, nil)
	if err == nil {
		t.Fatal("Wrong API call should return error")
	}

	// Anything
	_, err = Do("http://127.0.0.1:5984/couch_test_go/_compact", "POST", localCred, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func insertTestDoc(doc Identifiable, db *Database, t *testing.T) {
	err := db.Insert(doc)
	if err != nil {
		t.Fatal("Inserted new document, error:", err)
	}
}
func setUpDatabase(t *testing.T) *Database {
	db := database()
	if db.Exists() {
		err := db.DropDatabase()
		if err != nil {
			t.Fatal("Tried to delete existing database, failed with error:", err)
		}
	}
	err := db.Create()
	if err != nil {
		t.Fatal("Tried to create a new database, failed with error:", err)
	}
	return db
}

func tearDownDatabase(db *Database, t *testing.T) {
	err := db.DropDatabase()
	if err != nil {
		t.Fatal("Tried to delete existing database, failed with error:", err)
	}
}

func server() *Server {
	return NewServer(testHost, testCred)
}

func database() *Database {
	return server().Database(testDB)
}
