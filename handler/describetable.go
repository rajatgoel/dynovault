package handler

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func DescribeTable(ctx context.Context, s *state, input *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
	key := fmt.Sprintf("$table:%s", *input.TableName)
	_, err := s.kv.Get(ctx, []byte(key))
	if err != nil {
		return nil, err
	}

	// TODO: Fill tblDesc properly
	return &dynamodb.DescribeTableOutput{}, nil
}
