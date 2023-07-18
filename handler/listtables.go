package handler

import (
	"context"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func ListTables(ctx context.Context, s *state, input *dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error) {
	return &dynamodb.ListTablesOutput{}, nil
}
