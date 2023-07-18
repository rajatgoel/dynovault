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

func New(kv KVStore) http.Handler {
	return &ddbHandler{
		kv: kv,
	}
}

func (d *ddbHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	target := request.Header.Get("x-amz-target")
	switch target {
	case "DynamoDB_20120810.CreateTable":
		handle(writer, request, CreateTable)
	case "DynamoDB_20120810.ListTables":
		handle(writer, request, ListTables)
	case "DynamoDB_20120810.DeleteTable":
		handle(writer, request, DeleteTable)
	case "DynamoDB_20120810.PutItem":
		handle(writer, request, PutItem)
	case "DynamoDB_20120810.GetItem":
		handle(writer, request, GetItem)
	case "DynamoDB_20120810.UpdateItem":
		handle(writer, request, UpdateItem)
	case "DynamoDB_20120810.DeleteItem":
		handle(writer, request, DeleteItem)
	default:
		sendResponse(writer, 404, fmt.Sprintf("Unknown target method: %v", target))
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
		sendResponse(writer, 500, err.Error())
		return
	}

	var i I
	if err := json.Unmarshal(body, &i); err != nil {
		sendResponse(writer, 400, err.Error())
		return
	}

	resp, err := fn(request.Context(), &i)
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
