package couch

// CouchDB Design Document (not yet open to public)
type design struct {
	Doc
	Views map[string]view `json:"views"`
	// There a more elements to a design document, they will be added when they are implemented
}

// CouchDB View (not yet open to public)
type view struct {
	Map    string `json:"map,omitempty"`
	Reduce string `json:"reduce,omitempty"`
}

type ViewResult struct {
	Offset uint64
	Rows   []ViewResultRow
}

type ViewResultRow struct {
	Id    string
	Key   interface{}
	Value interface{}
}

func (r *ViewResultRow) ValueInt() int {
	num, _ := r.Value.(float64)
	return int(num)
}

func (db *Database) HasView(designId, viewId string) bool {
	ok, _ := checkHead(db.viewUrl(designId, viewId))
	return ok
}

func (db *Database) Query(designId, viewId string, options map[string]interface{}) (*ViewResult, error) {
	result := &ViewResult{}
	url := db.viewUrl(designId, viewId) + urlEncode(options)
	resp, err := request("GET", url, nil, &result)
	if err != nil {
		return nil, err
	}
	switch resp.StatusCode {
	case 404:
		err = ErrDocNotFound
	case 401:
		err = ErrNoReadPermission
	case 400:
		err = ErrBadRequest
	}
	return result, err
}

func newDesign() *design {
	d := &design{}
	d.Views = make(map[string]view)
	return d
}

func (db *Database) viewUrl(designId string, viewId string) string {
	return db.Url() + "/_design/" + designId + "/_view/" + viewId
}
