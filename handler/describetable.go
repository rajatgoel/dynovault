package handler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func DescribeTable(ctx context.Context, s *state, input *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
	key := fmt.Sprintf("$table:%s", *input.TableName)
	jsonValue, err := s.kv.Get(ctx, []byte(key))
	if err != nil {
		return nil, err
	}

	var td dynamodb.TableDescription
	if err := json.Unmarshal(jsonValue, &td); err != nil {
		return nil, err
	}

	return &dynamodb.DescribeTableOutput{
		Table: &td,
	}, nil
}
