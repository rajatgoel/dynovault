package handler

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"log"
)

func CreateTable(ctx context.Context, input *dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error) {
	log.Printf("Create table: %v\n", input.String())
	return nil, errors.New("not supported")
}
