package main

import (
	"flag"
	"net/http"

	"github.com/rajatgoel/dynovault/handler"
	"github.com/rajatgoel/dynovault/inmemory"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8779", "Server address")
	flag.Parse()

	kvStore := inmemory.New()
	reqHandler := handler.New(kvStore)
	http.Handle("/", reqHandler)

	_ = http.ListenAndServe(*addr, nil)
}
