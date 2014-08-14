// Package couch implements a client for a CouchDB database. Version 0.1
// focuses on basic interactions, replication, and conflict management.
//
// Setup
//
//  cred := NewCredentials("much", "safe")
//  s := NewServer("http://127.0.0.1:5984", cred)
//  db := s.Database("MyDatabase")
//  if !db.Exists() {
//    db.Create()
//  }
//
// Basics
//
// Every document in CouchDB has to be identifiable by an id and a revision id.
// Two datatypes implement the interface Identifiable, Doc and DynamicDoc. Doc can
// be used as an anonymous field in your own struct. DynamicDoc is a type alias
// for map[string]interface{}, use it when your documents have no implicit schema at all.
// To make code examples easier to follow, there will be no explicit error handling.
//
//  type Person struct {
//    couch.Doc
//    Name string
//  }
//

// Managing conflicting document revisions
// See http://docs.couchdb.org/en/latest/replication/conflicts.html

//  p := &DynamicDoc  // REaLLY?
//
//  p := &Person{Name : "Peter"}
//
//  // Because Id from couch.Doc is not set, insert will create a new document.
//  // After the insert, p will contain the id and revision id assigned to it.
//  db.Insert(p)
//  p.Name = "A new name"
//  db.Insert(p)    // Each insert writes the resulting id and revision into the given struct.
//
//  q := &Person{}
//  db.Retrieve(p.Id, q)
//
// Conflicts
//
//  count, _ := db.ConflictsCount()
//  docIds, _ := db.Conflicts()  // BAD NAME
//  conflict, _ := db.ConflictFor(docId)
//
//  var p []person
//  conflict.Revisions(&p)    // UnmarshalRevisions
//
//  conflict.SolveWith(anotherDoc)
//
// Continuous replication
//
//  repl, _ := db.ReplicateTo(anotherDb, true)
//  repl.Cancel()
//
package couch
