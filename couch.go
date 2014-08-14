package couch

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

var (
	ErrDatabaseUnknown      = errors.New("couch: Database doesn't exist")
	ErrDatabaseExists       = errors.New("couch: Database already exists")
	ErrNoWritePermission    = errors.New("couch: Write permission required")
	ErrNoReadPermission     = errors.New("couch: Read permission required")
	ErrNoAdmin              = errors.New("couch: Missing Admin privileges")
	ErrInvalidDatabaseName  = errors.New("couch: Invalid database name")
	ErrBadRequest           = errors.New("couch: Invalid request body or parameters")
	ErrDocNotFound          = errors.New("couch: Document not found")
	ErrDocConflict          = errors.New("couch: Document with the specified ID already exists or specified revision is not latest for target document")
	ErrInvalidJsonRequest   = errors.New("couch: JSON request was invalid")
	ErrMissingIdRev         = errors.New("couch: Missing Id or Rev")
	ErrNoConflictView       = errors.New("couch: No view for finding document conflicts")
	ErrBulkInsertAONFailed  = errors.New("couch: At least one document was rejected by validation during bulk insert with all_or_nothing enabled")
	ErrBulkInsertIncomplete = errors.New("couch: At least one document couldn't be inserted during atomic bulk insert (all_or_nothing disabled)")
	ErrNoConflict           = errors.New("couch: No revisions in conflict description found, probably already solved before.")
)

// CouchDB instance
type Server struct {
	url      string
	username string
	password string
}

// Database of a CouchDB instance
type Database struct {
	server *Server
	name   string
}

// Any document handled by CouchDB must be identifiable
// by an Id and a Revision, be it a struct (using Doc
// as anonymous field) or a DynamicDoc
type Identifiable interface {
	SetIdRev(id string, rev string)
	IdRev() (id string, rev string)
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
	Id  string `json:"_id,omitempty"`
	Rev string `json:"_rev,omitempty"`
	// Deleted bool   `json:"_deleted,omitempty"`  // TBD
}

// Type alias for map[string]interface{} representing
// a fully dynamic doc that still implements Identifiable
type DynamicDoc map[string]interface{}

// Container for bulk operations, use associated methods.
type DocBulk struct {
	AllOrNothing bool           `json:"all_or_nothing"`
	Docs         []Identifiable `json:"docs"`
}

// Implements Identifiable
func (ref *Doc) SetIdRev(id string, rev string) {
	ref.Id, ref.Rev = id, rev
}

// Implements Identifiable
func (ref *Doc) IdRev() (id string, rev string) {
	id, rev = ref.Id, ref.Rev
	return
}

// Implements Identifiable
func (m DynamicDoc) IdRev() (id string, rev string) {
	id, _ = m["_id"].(string)
	rev, _ = m["_rev"].(string)
	return
}

// Implements Identifiable
func (m DynamicDoc) SetIdRev(id string, rev string) {
	m["_id"] = id
	m["_rev"] = rev
}

// Returns a server handle
func NewServer(url, username, password string) *Server {
	return &Server{url: url, username: username, password: password}
}

// Returns a database handle
func (s *Server) Database(name string) *Database {
	return &Database{server: s, name: name}
}

// Create a new database
func (db *Database) Create() error {
	resp, err := request("PUT", db.Url(), nil, nil)
	if err != nil {
		return err
	}
	switch resp.StatusCode {
	case 201:
		break
	case 400:
		err = ErrBadRequest
	case 401:
		err = ErrNoAdmin
	case 412:
		err = ErrDatabaseExists
	default:
		err = fmt.Errorf("Unknown error after request, code: %v", resp.StatusCode)
	}
	return err
}

// Delete an existing database
func (db *Database) DropDatabase() error {
	resp, err := request("DELETE", db.Url(), nil, nil)
	if err != nil {
		return err
	}
	switch resp.StatusCode {
	case 200:
		break
	case 400:
		err = ErrBadRequest
	case 401:
		err = ErrNoAdmin
	case 404:
		err = ErrDatabaseUnknown
	default:
		err = fmt.Errorf("Unknown error after request, code: %v", resp.StatusCode)
	}
	return err
}

// Checks whether a database really exists.
func (db *Database) Exists() bool {
	exists, _ := checkHead(db.Url())
	return exists
}

type insertResult struct {
	Id  string
	Ok  bool
	Rev string
}

// Inserts a document as follows: If doc has an Id, it will edit the existing document,
// if not, create a new one. In case of an edit, the doc will be assigned the new revision id.
func (db *Database) Insert(doc Identifiable) error {
	var result insertResult
	var resp *http.Response
	var err error

	// New or edit
	id, _ := doc.IdRev()
	isEditing := id != ""
	if isEditing {
		resp, err = request("PUT", db.docUrl(id), doc, &result)
	} else {
		resp, err = request("POST", db.Url(), doc, &result)
	}
	if err != nil {
		return err
	}

	// Check response status
	switch resp.StatusCode {
	case 201, 202:
		doc.SetIdRev(result.Id, result.Rev)
	case 400:
		if isEditing {
			err = ErrBadRequest
		} else {
			err = ErrInvalidDatabaseName
		}
	case 401:
		err = ErrNoWritePermission
	case 404:
		err = ErrDatabaseUnknown
	case 409:
		err = ErrDocConflict
	default:
		err = fmt.Errorf("Unknown error after request, code: %v", resp.StatusCode)
	}
	return err
}

type bulkResult struct {
	Id     string
	Rev    string
	Ok     bool
	Error  string
	Reason string
}

// Inserts a bulk of documents at once. This transaction can have two semantics, all-or-nothing
// or per-document. See http://docs.couchdb.org/en/latest/api/database/bulk-api.html#bulk-documents-transaction-semantics
// After the transaction the method may return a new bulk of documents that couldn't be inserted. If this
// is the case you will still get an error reporting the exact issue.
func (db *Database) InsertBulk(bulk *DocBulk, allOrNothing bool) (*DocBulk, error) {
	var results []bulkResult
	bulk.AllOrNothing = allOrNothing
	resp, err := request("POST", db.Url()+"/_bulk_docs", bulk, &results)
	if err != nil {
		return bulk, err
	}
	switch resp.StatusCode {
	case 201:
		break
	case 400:
		err = ErrBadRequest
	case 417:
		err = ErrBulkInsertAONFailed
	case 500:
		err = ErrInvalidJsonRequest
	default:
		err = fmt.Errorf("Unknown error after request, code: %v", resp.StatusCode)
	}

	if err != nil {
		return bulk, err
	}

	// Update documents in bulk with ids and rev ids,
	// compile bulk of failed documents
	failedDocs := new(DocBulk)
	for i, result := range results {
		if result.Ok {
			bulk.Docs[i].SetIdRev(result.Id, result.Rev)
		} else {
			failedDocs.Add(bulk.Docs[i])
		}
	}
	if len(failedDocs.Docs) > 0 {
		err = ErrBulkInsertIncomplete
	}

	return failedDocs, err
}

// Retrieve a document by id using its latest revision,
// the result will be written into doc.
func (db *Database) Retrieve(docId string, doc Identifiable) error {
	return db.retrieve(docId, "", doc, nil)
}

// Retrieve a document by id and revision, the result will be written into doc
func (db *Database) RetrieveRevision(docId, revId string, doc Identifiable) error {
	return db.retrieve(docId, revId, doc, nil)
}

// Removes a document from the database. Doc needs to contain id and revision.
func (db *Database) Delete(doc Identifiable) error {
	id, rev := doc.IdRev()
	if id == "" || rev == "" {
		return ErrMissingIdRev
	}
	url := db.docUrl(id) + `?rev=` + rev
	resp, err := request("DELETE", url, nil, nil)
	if err != nil {
		return err
	}
	switch resp.StatusCode {
	case 200, 202:
		break
	case 404:
		err = ErrDocNotFound
	case 401:
		err = ErrNoWritePermission
	case 400:
		err = ErrBadRequest
	case 409:
		err = ErrDocConflict
	default:
		err = fmt.Errorf("Unknown error after request, code: %v", resp.StatusCode)
	}
	return err
}

// Returns the full url to a database
func (db *Database) Url() string {
	return db.server.url + "/" + db.name
}

// Use this only inside of a request body not as direct url
func (db *Database) UrlWithCredentials() string {
	result, _ := url.Parse(db.Url())
	result.User = url.UserPassword(db.server.username, db.server.password)
	return result.String()
}

// Returns the full url to a document
func (db *Database) docUrl(id string) string {
	return db.Url() + "/" + id
}

// Returns name of database
func (db *Database) Name() string {
	return db.name
}

// Add a document to a bulk of documents
func (bulk *DocBulk) Add(doc Identifiable) {
	bulk.Docs = append(bulk.Docs, doc)
}

// Find a document in a bulk of documents
func (bulk *DocBulk) Find(id string, rev string) Identifiable {
	for _, doc := range bulk.Docs {
		docId, docRev := doc.IdRev()
		if docId == id && docRev == rev {
			return doc
		}
	}
	return nil
}

func (db *Database) retrieve(id, revId string, doc interface{}, options map[string]interface{}) (err error) {
	if revId != "" {
		if options == nil {
			options = make(map[string]interface{})
		}
		options["rev"] = revId
	}
	url := db.docUrl(id) + urlEncode(options)
	resp, err := request("GET", url, nil, &doc)
	if err != nil {
		return err
	}
	switch resp.StatusCode {
	case 200, 304:
		break
	case 404:
		err = ErrDocNotFound
	case 401:
		err = ErrNoReadPermission
	case 400:
		err = ErrBadRequest
	default:
		err = fmt.Errorf("Unknown error after request, code: %v", resp.StatusCode)
	}
	return
}

func request(method, url string, jsonRequest interface{}, jsonResponse interface{}) (*http.Response, error) {
	// Prepare
	var requestBodyReader io.Reader
	if jsonRequest != nil {
		requestBody, err := json.Marshal(jsonRequest)
		if err != nil {
			return nil, err
		}
		requestBodyReader = bytes.NewReader(requestBody)
	}
	req, err := http.NewRequest(method, url, requestBodyReader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth("satowai", "karunka")

	// Send
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return resp, err
	}

	// Decode response body
	if jsonResponse != nil {
		err = json.NewDecoder(resp.Body).Decode(jsonResponse)
	}
	// var test map[string]interface{}
	// err = json.NewDecoder(resp.Body).Decode(&test)
	// fmt.Println(test)
	// if jsonResponse != nil {
	// 	jsonResponse = &test
	// }
	return resp, err
}

// Helper to make HEAD request
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

// Helper to encode map entries to url parameters
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
