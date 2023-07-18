package handler

import (
	"context"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func PutItem(ctx context.Context, s *state, input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	return &dynamodb.PutItemOutput{}, nil
}
