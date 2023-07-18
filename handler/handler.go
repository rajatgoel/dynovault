package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type state struct {
	kv KVStore
}

type ddbHandler struct {
	s *state
}

func New(kv KVStore) http.Handler {
	return &ddbHandler{
		s: &state{kv: kv},
	}
}

func (d *ddbHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	target := request.Header.Get("x-amz-target")
	switch target {
	case "DynamoDB_20120810.CreateTable":
		handle(writer, request, d.s, CreateTable)
	case "DynamoDB_20120810.ListTables":
		handle(writer, request, d.s, ListTables)
	case "DynamoDB_20120810.DeleteTable":
		handle(writer, request, d.s, DeleteTable)
	case "DynamoDB_20120810.PutItem":
		handle(writer, request, d.s, PutItem)
	case "DynamoDB_20120810.GetItem":
		handle(writer, request, d.s, GetItem)
	case "DynamoDB_20120810.UpdateItem":
		handle(writer, request, d.s, UpdateItem)
	case "DynamoDB_20120810.DeleteItem":
		handle(writer, request, d.s, DeleteItem)
	default:
		sendResponse(writer, 404, fmt.Sprintf("Unknown target method: %v", target))
	}
}

func handle[I any, O any](
	writer http.ResponseWriter,
	request *http.Request,
	s *state,
	fn func(context.Context, *state, *I) (*O, error),
) {
	body, err := io.ReadAll(request.Body)
	_ = request.Body.Close()
	if err != nil {
		sendResponse(writer, 500, err.Error())
		return
	}

	var i I
	if err := json.Unmarshal(body, &i); err != nil {
		sendResponse(writer, 400, err.Error())
		return
	}

	resp, err := fn(request.Context(), s, &i)
	if err != nil {
		sendResponse(writer, 500, err.Error())
		return
	}

	jsonResp, err := json.Marshal(resp)
	if err != nil {
		sendResponse(writer, 500, err.Error())
		return
	}

	sendResponse(writer, 200, string(jsonResp))
}

func sendResponse(writer http.ResponseWriter, statusCode int, message string) {
	writer.WriteHeader(statusCode)
	writer.Header().Set("Content-Type", "application/json")
	_, _ = writer.Write([]byte(message))
}
