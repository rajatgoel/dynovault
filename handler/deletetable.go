package handler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func DeleteTable(ctx context.Context, s *state, input *dynamodb.DeleteTableInput) (*dynamodb.DeleteTableOutput, error) {
	key := fmt.Sprintf("$table:%s", *input.TableName)
	jsonValue, err := s.kv.Get(ctx, []byte(key))
	if err != nil {
		return nil, err
	}

	var td dynamodb.TableDescription
	if err = json.Unmarshal(jsonValue, &td); err != nil {
		return nil, err
	}

	if err = s.kv.Delete(ctx, []byte(key)); err != nil {
		return nil, err
	}

	return &dynamodb.DeleteTableOutput{
		TableDescription: &td,
	}, nil
}
