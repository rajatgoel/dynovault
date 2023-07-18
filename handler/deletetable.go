package handler

import (
	"context"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func DeleteTable(ctx context.Context, input *dynamodb.DeleteTableInput) (*dynamodb.DeleteTableOutput, error) {
	return &dynamodb.DeleteTableOutput{}, nil
}
