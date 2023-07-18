package handler

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func ListTables(ctx context.Context, input *dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error) {
	log.Printf("List tables: %v\n", input.String())
	return &dynamodb.ListTablesOutput{}, nil
}
