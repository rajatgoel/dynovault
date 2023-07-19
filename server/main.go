package main

import (
	"flag"
	"net/http"
	"os"

	"github.com/gorilla/handlers"

	"github.com/rajatgoel/dynovault/handler"
	"github.com/rajatgoel/dynovault/inmemory"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8779", "Server address")
	flag.Parse()

	kvStore := inmemory.New()
	reqHandler := handler.New(kvStore)
	http.Handle("/", handlers.LoggingHandler(os.Stdout, reqHandler))

	_ = http.ListenAndServe(*addr, nil)
}
