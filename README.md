# Couch 

[![Build Status](https://travis-ci.org/patrickjuchli/couch.png)](https://travis-ci.org/patrickjuchli/couch) [![GoDoc](https://godoc.org/github.com/patrickjuchli/couch?status.svg)](http://godoc.org/github.com/patrickjuchli/couch)

This is a CouchDB client for Go, it focuses on basic operations, proper conflict management, error handling and replication. 

Not yet part of this are attachment handling, general statistics and optimizations, change detection and creating views. Most of the features are accessible using the generic Do() function, though.

Suggestions and critique are welcome.

## Documentation

Documentation and API Reference can be found on [godoc.org](http://godoc.org/github.com/patrickjuchli/couch)

## Installation

Install couch using the "go get" command:

    go get github.com/patrickjuchli/couch

The Go distribution is the only dependency.