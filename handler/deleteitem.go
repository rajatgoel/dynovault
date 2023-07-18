package handler

import (
	"context"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func DeleteItem(ctx context.Context, s *state, input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	return &dynamodb.DeleteItemOutput{}, nil
}
