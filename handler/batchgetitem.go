package handler

import (
	"context"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func BatchGetItem(
	ctx context.Context,
	s *state,
	input *dynamodb.BatchGetItemInput,
) (*dynamodb.BatchGetItemOutput, error) {
	return &dynamodb.BatchGetItemOutput{}, nil
}
