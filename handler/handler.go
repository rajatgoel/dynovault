package handler

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type ddbHandler struct {
	kv KVStore
}

func New(kv KVStore) *ddbHandler {
	return &ddbHandler{
		kv: kv,
	}
}

type dynamodbRequest struct {
	Method    string
	InputData map[string]interface{}
}

func getDdbMethod(request *http.Request) string {
	return strings.Split(request.Header.Get("x-amz-target"), ".")[1]
}

func ddbInputFromRequestBody(requestBody []byte) (map[string]interface{}, error) {
	var input map[string]interface{}
	err := json.Unmarshal(requestBody, &input)
	if err != nil {
		return nil, err
	}
	return input, nil
}

func (d *ddbHandler) ServeHTTP(write http.ResponseWriter, request *http.Request) {
	method := getDdbMethod(request)

	body, err := io.ReadAll(request.Body)
	request.Body.Close()

	if err != nil {
		write.WriteHeader(500)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(404)

	inputData, err := ddbInputFromRequestBody(body)
	if err != nil {
		write.WriteHeader(500)
		return
	}

	ddbRequest := &dynamodbRequest{
		Method:    method,
		InputData: inputData,
	}

	fmt.Printf("DDB Method: %s\n", ddbRequest.Method)
	fmt.Printf("DDB Input: %s\n\n", ddbRequest.InputData)

	write.WriteHeader(404)
}
