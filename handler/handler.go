package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type ddbHandler struct {
	kv KVStore
}

func New(kv KVStore) *ddbHandler {
	return &ddbHandler{
		kv: kv,
	}
}

func (d *ddbHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	target := request.Header.Get("x-amz-target")
	switch target {
	case "DynamoDB_20120810.CreateTable":
		handle(writer, request, CreateTable)
	default:
		writer.WriteHeader(404)
		writer.Header().Set("Content-Type", "application/json")
		writer.Write([]byte(fmt.Sprintf("Unkonwn target method: %v", target)))
	}
}

func handle[I any, O any](
	writer http.ResponseWriter,
	request *http.Request,
	fn func(context.Context, *I) (*O, error),
) {
	body, err := io.ReadAll(request.Body)
	_ = request.Body.Close()
	if err != nil {
		writer.WriteHeader(500)
		writer.Write([]byte(err.Error()))
		return
	}

	var i I
	if err := json.Unmarshal(body, &i); err != nil {
		writer.WriteHeader(400)
		writer.Write([]byte(err.Error()))
		return
	}

	resp, err := fn(request.Context(), &i)
	if err != nil {
		writer.WriteHeader(500)
		writer.Write([]byte(err.Error()))
		return
	}

	jsonResp, err := json.Marshal(resp)
	if err != nil {
		writer.WriteHeader(500)
		writer.Write([]byte(err.Error()))
		return
	}

	writer.WriteHeader(200)
	writer.Header().Set("Content-Type", "application/json")
	writer.Write(jsonResp)
}
