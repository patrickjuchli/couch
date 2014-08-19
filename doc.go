// Package couch implements a client for a CouchDB database.
//
// Version 0.1 focuses on basic operations, proper conflict management, error handling and
// replication. Not part of this version are attachment handling, general
// statistics and optimizations, change detection and creating views. Most of
// the features are accessible using the generic Do() function, though.
//
//
// Getting started:
//
//  cred := couch.NewCredentials("user_notsosafe", "password_withoutssl")
//  s := couch.NewServer("http://127.0.0.1:5984", cred)
//  db := s.Database("MyDatabase")
//
//  if !db.Exists() {
//    db.Create()
//  }
//
// Basics
//
// Every document in CouchDB has to be identifiable by a document id and a revision id.
// Two types already implement this interface called Identifiable: Doc and DynamicDoc. Doc can
// be used as an anonymous field in your own struct. DynamicDoc is a type alias
// for map[string]interface{}, use it when your documents have no implicit schema at all.
// To make code examples easier to follow, there will be no explicit error handling in
// these examples even though it's fully supported throughout the API.
//
//  type Person struct {
//    couch.Doc
//    Name string
//  }
//
// Insert() will create a new document if it doesn't have an id yet:
//
//  p := &Person{Name : "Peter"}
//  db.Insert(p)
//
// After the operation the final id and revision id will be written back to p. That's
// why you can now just edit p and call Insert() again which will save the same document
// under a new revision.
//
//  p.Name = "Anna"
//  db.Insert(p)
//
// After this edit, p will contain the latest revision id. Note that it is possible that this
// second edit fails because someone else edited and saved the same document in the meantime.
// You will be notified of this in form of an error and you should then first retrieve the
// latest document revision to see the changes of this lost update:
//
//  db.Retrieve(p.ID, p)
//
// CouchDB doesn't edit documents in-place but adds a complete revision for each edit. That's
// why you will be correctly informed of any lost update.
//
// Conflicts
//
// Because CouchDB supports multi-master replication of databases, it is possible that conflicts
// like the one described above can't be avoided. CouchDB is not going to interrupt replication
// because of a lost update.
//
// Let's say you have two instances running,
// maybe a central one and a mobile one and both are kept in sync by replication. Now let's assume
// you edit a document on your mobile DB and someone else edits the same document on the central DB.
// After you've come online again, you use bi-directional replication to sync the databases.
// CouchDB will now create a branch structure for your document, similar to version control systems.
// Your document has two conflicting revisions and in this case they can't necessarily be resolved
// automatically. This client helps you with a number of methods to resolve such an issue quickly.
// Read more about the conflict model http://docs.couchdb.org/en/latest/replication/conflicts.html
//
// Continuing with above example, replicate the database:
//
//  anotherDB := s.Database("sharedBackup")
//  db.ReplicateTo(anotherDB, false)
//
// Now, on the other database, edit the document (note that it has the same id there):
//
//  p.Name = "AnotherAnna"
//  anotherDB.Insert(p)
//
// Now edit the document on the first database. Retrieve it first to make sure it
// has the correct revision id:
//
//  db.Retrieve(p.ID, p)
//  p.Name = "LatestAnna"
//  db.Insert(p)
//
// Now replicate anotherDB back to our first database:
//
//  anotherDB.ReplicateTo(db, false)
//
// Now we have two conflicting versions of a document. Only you as the editor can decide
// whether "LatestAnna" or "AnotherAnna" is correct. To detect this conflict there are a number
// of methods. First, you can just ask a document:
//
//  conflict, _ := db.ConflictFor(p.ID)
//
// You probably want to have a look at the revisions in your preferred format, use Revisions()
// to unmarshal the revision data into a slice of a custom data type:
//
//  var revs []Person
//  conflict.Revisions(&revs)
//
// Pick one of the revisions or create a new document to solve the conflict:
//
//  solution := &Person{Name:"Anna"}
//  conflict.SolveWith(solution)
//
// That's it. You can detect conflicts like these throughout your database using:
//
//  num := db.ConflictsCount()
//  docIDs := db.Conflicts()
//
//
// Error handling
//
// Errors returned by CouchDB will be converted into a Go error. Its regular Error() method will
// then return a combination of the shortform (e.g. bad_request) as well as the longer and more
// specific description. To be able to identify a specific error within your application, use ErrorType() to get
// the shortform only.
package couch
