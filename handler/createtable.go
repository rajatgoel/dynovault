package handler

import (
	"context"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func CreateTable(ctx context.Context, s *state, input *dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error) {
	return &dynamodb.CreateTableOutput{}, nil
}
