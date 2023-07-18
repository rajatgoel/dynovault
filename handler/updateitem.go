package handler

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func UpdateItem(ctx context.Context, s *state, input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	log.Printf("Update item : %v\n", input.String())
	log.Printf("Table name: %v\n", *input.TableName)
	return &dynamodb.UpdateItemOutput{}, nil
}
