package handler

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func DeleteItem(ctx context.Context, s *state, input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	key := *input.TableName
	for k, v := range input.Key {
		key = fmt.Sprintf("%s:%s-%s", key, k, *v.S)
	}
	if err := s.kv.Delete(ctx, []byte(key)); err != nil {
		return nil, err
	}
	return &dynamodb.DeleteItemOutput{}, nil
}
