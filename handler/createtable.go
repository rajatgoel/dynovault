package handler

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func CreateTable(ctx context.Context, s *state, input *dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error) {
	log.Printf("Create table: %v\n", input.String())
	log.Printf("Table name: %v\n", *input.TableName)
	return &dynamodb.CreateTableOutput{}, nil
}
