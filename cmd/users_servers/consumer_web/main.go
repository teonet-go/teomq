package main

import (
	"log"
	"net/http"
)

func main() {

	// Handle static files in the current directory
	http.Handle("/", http.FileServer(http.Dir("./")))

	// Start web server
	log.Fatal(http.ListenAndServe(":8094", nil))
}
