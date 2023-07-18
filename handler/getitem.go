package handler

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func GetItem(ctx context.Context, input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	log.Printf("Get item : %v\n", input.String())
	log.Printf("Table name: %v\n", *input.TableName)
	return &dynamodb.GetItemOutput{}, nil
}
