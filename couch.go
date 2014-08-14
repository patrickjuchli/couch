package couch

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
)

// CouchDB instance
type Server struct {
	url  string
	cred *Credentials
}

// Database of a CouchDB instance
type Database struct {
	server *Server
	name   string
	cred   *Credentials
}

// Any document handled by CouchDB must be identifiable
// by an ID and a Revision, be it a struct (using Doc
// as anonymous field) or a DynamicDoc
type Identifiable interface {
	SetIDRev(id string, rev string)
	IDRev() (id string, rev string)
}

// Defines basic struct for CouchDB document, should be added
// as an anonymous field to your custom struct.
//
// Example:
//  type MyDocStruct struct {
//   couch.Doc
//   Title string
//  }
type Doc struct {
	ID  string `json:"_id,omitempty"`
	Rev string `json:"_rev,omitempty"`
}

// Type alias for map[string]interface{} representing
// a fully dynamic doc that still implements Identifiable
type DynamicDoc map[string]interface{}

// Access credentials
type Credentials struct {
	user     string
	password string
}

// Implements Identifiable
func (ref *Doc) SetIDRev(id string, rev string) {
	ref.ID, ref.Rev = id, rev
}

// Implements Identifiable
func (ref *Doc) IDRev() (id string, rev string) {
	id, rev = ref.ID, ref.Rev
	return
}

// Implements Identifiable
func (m DynamicDoc) IDRev() (id string, rev string) {
	id, _ = m["_id"].(string)
	rev, _ = m["_rev"].(string)
	return
}

// Implements Identifiable
func (m DynamicDoc) SetIDRev(id string, rev string) {
	m["_id"] = id
	m["_rev"] = rev
}

// CouchDB error description
type couchError struct {
	Type   string `json:"error"`
	Reason string `json:"reason"`
}

func (e couchError) Error() string {
	return "couchdb: " + e.Type + " (" + e.Reason + ")"
}

// If an error originated from CouchDB, this convenience function
// returns its shortform error type (e.g. bad_request). If the error
// is from a different source, the function will return an empty string.
func ErrorType(err error) string {
	cErr, _ := err.(couchError)
	return cErr.Type
}

// Returns a server handle
func NewServer(url string, cred *Credentials) *Server {
	return &Server{url: url, cred: cred}
}

// Returns new credentials you can use for server and/or database operations.
func NewCredentials(user, password string) *Credentials {
	return &Credentials{user: user, password: password}
}

// Returns a database handle
func (s *Server) Database(name string) *Database {
	return &Database{server: s, name: name}
}

// Returns the credentials associated with the database. If there aren't any
// it will return the ones associated with the server.
func (db *Database) Cred() *Credentials {
	if db.cred != nil {
		return db.cred
	}
	return db.server.cred
}

// Create a new database
func (db *Database) Create() error {
	_, err := Do(db.Url(), "PUT", db.Cred(), nil, nil)
	return err
}

// Delete an existing database
func (db *Database) DropDatabase() error {
	_, err := Do(db.Url(), "DELETE", db.Cred(), nil, nil)
	return err
}

// Checks whether a database really exists.
func (db *Database) Exists() bool {
	exists, _ := checkHead(db.Url())
	return exists
}

// CouchDB result of document insert
type insertResult struct {
	ID  string
	Ok  bool
	Rev string
}

// Inserts a document as follows: If doc has an ID, it will edit the existing document,
// if not, create a new one. In case of an edit, the doc will be assigned the new revision id.
func (db *Database) Insert(doc Identifiable) error {
	var result insertResult
	var err error
	id, _ := doc.IDRev()
	if id == "" {
		_, err = Do(db.Url(), "POST", db.Cred(), doc, &result)
	} else {
		_, err = Do(db.docUrl(id), "PUT", db.Cred(), doc, &result)
	}
	if err != nil {
		return err
	}
	doc.SetIDRev(result.ID, result.Rev)
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

// Inserts a bulk of documents at once. This transaction can have two semantics, all-or-nothing
// or per-document. See http://docs.couchdb.org/en/latest/api/database/bulk-api.html#bulk-documents-transaction-semantics
// After the transaction the method may return a new bulk of documents that couldn't be inserted.
// If this is the case you will still get an error reporting the issue.
func (db *Database) InsertBulk(bulk *DocBulk, allOrNothing bool) (*DocBulk, error) {
	var results []bulkResult
	bulk.AllOrNothing = allOrNothing
	_, err := Do(db.Url()+"/_bulk_docs", "POST", db.Cred(), bulk, &results)

	// Update documents in bulk with ids and rev ids,
	// compile bulk of failed documents
	failedDocs := new(DocBulk)
	for i, result := range results {
		if result.Ok {
			bulk.Docs[i].SetIDRev(result.ID, result.Rev)
		} else {
			failedDocs.Add(bulk.Docs[i])
		}
	}
	if len(failedDocs.Docs) > 0 {
		err = errors.New("Bulk insert incomplete")
	}

	return failedDocs, err
}

// Removes a document from the database.
func (db *Database) Delete(docID, revID string) error {
	url := db.docUrl(docID) + `?rev=` + revID
	_, err := Do(url, "DELETE", db.Cred(), nil, nil)
	return err
}

// Returns the full url to a database
func (db *Database) Url() string {
	return db.server.url + "/" + db.name
}

// Returns the full url to a document
func (db *Database) docUrl(id string) string {
	return db.Url() + "/" + id
}

// Returns name of database
func (db *Database) Name() string {
	return db.name
}

// Retrieve a document by id using its latest revision,
// the result will be written into doc.
func (db *Database) Retrieve(docID string, doc Identifiable) error {
	return db.retrieve(docID, "", doc, nil)
}

// Retrieve a document by id and revision, the result will be written into doc
func (db *Database) RetrieveRevision(docID, revID string, doc Identifiable) error {
	return db.retrieve(docID, revID, doc, nil)
}

func (db *Database) retrieve(id, revID string, doc interface{}, options map[string]interface{}) error {
	if revID != "" {
		if options == nil {
			options = make(map[string]interface{})
		}
		options["rev"] = revID
	}
	url := db.docUrl(id) + urlEncode(options)
	_, err := Do(url, "GET", db.Cred(), nil, &doc)
	return err
}

// Generic CouchDB request. If CouchDB returns an error description, it
// will not be unmarshaled into response but returned as a regular Go error.
func Do(url, method string, cred *Credentials, body, response interface{}) (*http.Response, error) {

	// Prepare json request
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
	req.Header.Set("Accept", "application/json") // TODO Header should be settable
	if cred != nil {
		req.SetBasicAuth(cred.user, cred.password)
	}

	// Make request
	resp, err := http.DefaultClient.Do(req) // TODO Client should be settable, maybe in credentials and rename credentials to something else?
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
		err = json.Unmarshal(respBody, response) // TODO unmarshaling twice not so ideal but no way around?
	}
	return resp, err
}
