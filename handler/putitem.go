package handler

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func PutItem(ctx context.Context, s *state, input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	log.Printf("Put item : %v\n", input.String())
	log.Printf("Table name: %v\n", *input.TableName)
	return &dynamodb.PutItemOutput{}, nil
}
