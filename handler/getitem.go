package handler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func GetItem(ctx context.Context, s *state, input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	key := *input.TableName
	for k, v := range input.Key {
		key = fmt.Sprintf("%s:%s-%s", key, k, *v.S)
	}
	jsonValue, err := s.kv.Get(ctx, []byte(key))
	if err != nil {
		return &dynamodb.GetItemOutput{}, nil
	}
	var value map[string]*dynamodb.AttributeValue
	if err := json.Unmarshal(jsonValue, &value); err != nil {
		return nil, err
	}
	return &dynamodb.GetItemOutput{
		Item: value,
	}, nil
}
