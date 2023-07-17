package handler

import "net/http"

type ddbHandler struct {
}

func New() *ddbHandler {
	return &ddbHandler{}
}

func (d *ddbHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.WriteHeader(404)
}
