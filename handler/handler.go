package handler

import (
	"fmt"
	"io"
	"net/http"
)

type ddbHandler struct {
}

func New() *ddbHandler {
	return &ddbHandler{}
}

func (d *ddbHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	fmt.Println("Received HTTP Request:")
	fmt.Printf("%v %v %v\n", request.Method, request.URL, request.Proto)
	fmt.Printf("Host: %v\n", request.Host)
	for name, headers := range request.Header {
		for _, h := range headers {
			fmt.Printf("%v: %v\n", name, h)
		}
	}
	body, err := io.ReadAll(request.Body)
	request.Body.Close()
	if err != nil {
		writer.WriteHeader(500)
		return
	}
	fmt.Printf("%s\n\n", body)

	writer.WriteHeader(404)
}
