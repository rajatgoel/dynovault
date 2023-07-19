package handler

import (
	"context"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func BatchWriteItem(
	ctx context.Context,
	s *state,
	input *dynamodb.BatchWriteItemInput,
) (*dynamodb.BatchWriteItemOutput, error) {
	return &dynamodb.BatchWriteItemOutput{}, nil
}
