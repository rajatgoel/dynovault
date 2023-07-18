package handler

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	log.Printf("Delete item : %v\n", input.String())
	log.Printf("Table name: %v\n", *input.TableName)
	return &dynamodb.DeleteItemOutput{}, nil
}
