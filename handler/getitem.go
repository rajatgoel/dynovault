package handler

import (
	"context"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func GetItem(ctx context.Context, s *state, input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	return &dynamodb.GetItemOutput{}, nil
}
