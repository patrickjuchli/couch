package couch

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
)

// Container for bulk operations, use associated methods.
type DocBulk struct {
	Docs         []Identifiable `json:"docs"`
	AllOrNothing bool           `json:"all_or_nothing"`
}

// Add a document to a bulk of documents
func (bulk *DocBulk) Add(doc Identifiable) {
	bulk.Docs = append(bulk.Docs, doc)
}

// Find a document in a bulk of documents
func (bulk *DocBulk) Find(id string, rev string) Identifiable {
	for _, doc := range bulk.Docs {
		docID, docRev := doc.IDRev()
		if docID == id && docRev == rev {
			return doc
		}
	}
	return nil
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

// Helper to make quick HEAD request
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
