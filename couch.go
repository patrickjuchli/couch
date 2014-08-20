package couch

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
)

// Server represents a CouchDB instance.
type Server struct {
	url  string
	cred *Credentials
}

// NewServer returns a handle to a CouchDB instance.
func NewServer(url string, cred *Credentials) *Server {
	return &Server{url: url, cred: cred}
}

// Database returns a reference to a database. This method will
// not check if the database really exists.
func (s *Server) Database(name string) *Database {
	return &Database{server: s, name: name}
}

// URL returns the host (including its port) of a CouchDB instance.
func (s *Server) URL() string {
	return s.url
}

// Cred returns credentials associated with a CouchDB instance.
func (s *Server) Cred() *Credentials {
	return s.cred
}

// ActiveTasks returns all currently active tasks of a CouchDB instance.
func (s *Server) ActiveTasks() ([]Task, error) {
	var tasks []Task
	_, err := Do(s.URL()+"/_active_tasks", "GET", s.Cred(), nil, &tasks)
	return tasks, err
}

// Credentials represents access credentials.
type Credentials struct {
	user     string
	password string
}

// NewCredentials returns new credentials you can use for server and/or database operations.
func NewCredentials(user, password string) *Credentials {
	return &Credentials{user: user, password: password}
}

// Identifiable is the only interface a data structure must satisfy to
// be used as a CouchDB document.
type Identifiable interface {

	// SetIDRev sets the document id and revision id
	SetIDRev(id string, rev string)

	// IDRev returns the document id and revision id
	IDRev() (id string, rev string)
}

// Doc defines a basic struct for CouchDB documents. Add it
// as an anonymous field to your custom struct.
type Doc struct {
	ID  string `json:"_id,omitempty"`
	Rev string `json:"_rev,omitempty"`
}

// Implement Identifiable
func (ref *Doc) SetIDRev(id string, rev string) {
	ref.ID, ref.Rev = id, rev
}

// Implement Identifiable
func (ref *Doc) IDRev() (id string, rev string) {
	id, rev = ref.ID, ref.Rev
	return
}

// DynamicDoc can be used for CouchDB documents without
// any implicit schema.
type DynamicDoc map[string]interface{}

// Implement Identifiable
func (m DynamicDoc) IDRev() (id string, rev string) {
	id, _ = m["_id"].(string)
	rev, _ = m["_rev"].(string)
	return
}

// Implement Identifiable
func (m DynamicDoc) SetIDRev(id string, rev string) {
	m["_id"] = id
	m["_rev"] = rev
}

// Task describes an active task running on an instance,
// like a continuous replication or indexing.
type Task map[string]interface{}

// Database represents a database of a CouchDB instance.
type Database struct {
	name   string
	cred   *Credentials
	server *Server
}

// Cred returns the credentials associated with the database. If there aren't any
// it will return the ones associated with the server.
func (db *Database) Cred() *Credentials {
	if db.cred != nil {
		return db.cred
	}
	return db.server.Cred()
}

// SetCred sets the credentials used for operations with the database.
func (db *Database) SetCred(c *Credentials) {
	db.cred = c
}

// Server returns the CouchDB instance the database is located on.
func (db *Database) Server() *Server {
	return db.server
}

// Create a new database on the CouchDB instance.
func (db *Database) Create() error {
	_, err := Do(db.URL(), "PUT", db.Cred(), nil, nil)
	return err
}

// DropDatabase deletes a database.
func (db *Database) DropDatabase() error {
	_, err := Do(db.URL(), "DELETE", db.Cred(), nil, nil)
	return err
}

// Exists returns true if a database really exists.
func (db *Database) Exists() bool {
	exists, _ := checkHead(db.URL())
	return exists
}

// CouchDB result of document insert
type insertResult struct {
	ID  string
	Ok  bool
	Rev string
}

// Insert a document as follows: If doc has an ID, it will edit the existing document,
// if not, create a new one. In case of an edit, the doc will be assigned the new revision id.
func (db *Database) Insert(doc Identifiable) error {
	var result insertResult
	var err error
	id, _ := doc.IDRev()
	if id == "" {
		_, err = Do(db.URL(), "POST", db.Cred(), doc, &result)
	} else {
		_, err = Do(db.docURL(id), "PUT", db.Cred(), doc, &result)
	}
	if err != nil {
		return err
	}
	doc.SetIDRev(result.ID, result.Rev)
	return nil
}

// Delete removes a document from the database.
func (db *Database) Delete(docID, revID string) error {
	url := db.docURL(docID) + `?rev=` + revID
	_, err := Do(url, "DELETE", db.Cred(), nil, nil)
	return err
}

// Url returns the absolute url to a database
func (db *Database) URL() string {
	return db.server.url + "/" + db.name
}

// DocUrl returns the absolute url to a document
func (db *Database) docURL(id string) string {
	return db.URL() + "/" + id
}

// Name of database
func (db *Database) Name() string {
	return db.name
}

// Retrieve gets the latest revision document of a document, the result will be written into doc
func (db *Database) Retrieve(docID string, doc Identifiable) error {
	return db.retrieve(docID, "", doc, nil)
}

// RetrieveRevision gets a specific revision of a document, the result will be written into doc
func (db *Database) RetrieveRevision(docID, revID string, doc Identifiable) error {
	return db.retrieve(docID, revID, doc, nil)
}

// Generic method to get one or more documents
func (db *Database) retrieve(id, revID string, doc interface{}, options map[string]interface{}) error {
	if revID != "" {
		if options == nil {
			options = make(map[string]interface{})
		}
		options["rev"] = revID
	}
	url := db.docURL(id) + urlEncode(options)
	_, err := Do(url, "GET", db.Cred(), nil, &doc)
	return err
}

// Bulk is a document container for bulk operations.
type Bulk struct {
	Docs         []Identifiable `json:"docs"`
	AllOrNothing bool           `json:"all_or_nothing"`
}

// Add a document to a bulk of documents
func (bulk *Bulk) Add(doc Identifiable) {
	bulk.Docs = append(bulk.Docs, doc)
}

// Find a document in a bulk of documents
func (bulk *Bulk) Find(id, rev string) Identifiable {
	for _, doc := range bulk.Docs {
		docID, docRev := doc.IDRev()
		if docID == id && docRev == rev {
			return doc
		}
	}
	return nil
}

// CouchDB result of bulk insert
type bulkResult struct {
	ID     string
	Rev    string
	Ok     bool
	Error  string
	Reason string
}

// InsertBulk inserts a bulk of documents at once. This transaction can have two semantics, all-or-nothing
// or per-document. See http://docs.couchdb.org/en/latest/api/database/bulk-api.html#bulk-documents-transaction-semantics
// After the transaction the method may return a new bulk of documents that couldn't be inserted.
// If this is the case you will still get an error reporting the issue.
func (db *Database) InsertBulk(bulk *Bulk, allOrNothing bool) (*Bulk, error) {
	var results []bulkResult
	bulk.AllOrNothing = allOrNothing
	_, err := Do(db.URL()+"/_bulk_docs", "POST", db.Cred(), bulk, &results)

	// Update documents in bulk with ids and rev ids,
	// compile bulk of failed documents
	failedDocs := new(Bulk)
	for i, result := range results {
		if result.Ok {
			bulk.Docs[i].SetIDRev(result.ID, result.Rev)
		} else {
			failedDocs.Add(bulk.Docs[i])
		}
	}
	if len(failedDocs.Docs) > 0 {
		err = errors.New("bulk insert incomplete")
	}

	return failedDocs, err
}

// Generic CouchDB request. If CouchDB returns an error description, it
// will not be unmarshaled into response but returned as a regular Go error.
func Do(url, method string, cred *Credentials, body, response interface{}) (*http.Response, error) {

	// Prepare json request body
	var bodyReader io.Reader
	if body != nil {
		json, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		bodyReader = bytes.NewReader(json)
	}

	// Prepare request
	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if cred != nil {
		req.SetBasicAuth(cred.user, cred.password)
	}

	// Make request
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return resp, err
	}

	// Catch error response in json body
	respBody, _ := ioutil.ReadAll(resp.Body)
	var cErr couchError
	json.Unmarshal(respBody, &cErr)
	if cErr.Type != "" {
		return nil, cErr
	}
	if response != nil {
		err = json.Unmarshal(respBody, response)
	}
	return resp, err
}

// CouchDB error description
type couchError struct {
	Type   string `json:"error"`
	Reason string `json:"reason"`
}

// Error implements the error interface.
func (e couchError) Error() string {
	return "couchdb: " + e.Type + " (" + e.Reason + ")"
}

// ErrorType returns the shortform of a CouchDB error, e.g. bad_request.
// If the error didn't originate from CouchDB, the function will return an empty string.
func ErrorType(err error) string {
	cErr, _ := err.(couchError)
	return cErr.Type
}

// Check if HEAD response of a url succeeds
func checkHead(url string) (bool, error) {
	resp, err := http.Head(url)
	if err != nil {
		return false, err
	}
	if resp.StatusCode != 200 {
		return false, nil
	}
	return true, nil
}

// Encode map entries to a string that can be used as parameters to a url
func urlEncode(options map[string]interface{}) string {
	n := len(options)
	if n == 0 {
		return ""
	}
	var buf bytes.Buffer
	buf.WriteString(`?`)
	for k, v := range options {
		var s string
		switch v.(type) {
		case string:
			s = fmt.Sprintf(`%s=%s&`, k, url.QueryEscape(v.(string)))
		case int:
			s = fmt.Sprintf(`%s=%d&`, k, v)
		case bool:
			s = fmt.Sprintf(`%s=%v&`, k, v)
		}
		buf.WriteString(s)
	}
	buf.Truncate(buf.Len() - 1)
	return buf.String()
}
