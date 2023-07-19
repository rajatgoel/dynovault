package handler

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func CreateTable(ctx context.Context, s *state, input *dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error) {
	// TODO: Check if table already exists
	// TODO: Fill TableDescription with useful data

	key := fmt.Sprintf("$table:%s", *input.TableName)
	value := &dynamodb.TableDescription{}

	if err := s.kv.Put(ctx, []byte(key), []byte(value.String())); err != nil {
		return nil, err
	}

	return &dynamodb.CreateTableOutput{
		TableDescription: value,
	}, nil
}
