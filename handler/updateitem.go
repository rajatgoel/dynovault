package handler

import (
	"context"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func UpdateItem(ctx context.Context, s *state, input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	return &dynamodb.UpdateItemOutput{}, nil
}
