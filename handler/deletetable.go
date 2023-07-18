package handler

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func DeleteTable(ctx context.Context, input *dynamodb.DeleteTableInput) (*dynamodb.DeleteTableOutput, error) {
	log.Printf("Delete table: %v\n", input.String())
	log.Printf("Table name: %v\n", *input.TableName)
	return &dynamodb.DeleteTableOutput{}, nil
}
