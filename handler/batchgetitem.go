package handler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func BatchGetItem(
	ctx context.Context,
	s *state,
	input *dynamodb.BatchGetItemInput,
) (*dynamodb.BatchGetItemOutput, error) {
	responses := map[string][]map[string]*dynamodb.AttributeValue{}

	for tableName, requestItem := range input.RequestItems {
		key := tableName
		for _, attr := range requestItem.Keys {
			// Flatten the keys into one string
			// TODO: ordering may mess us up here
			for k, v := range attr {
				key = fmt.Sprintf("%s:%s-%s", key, k, *v.S)
			}
			jsonValue, err := s.kv.Get(ctx, []byte(key))
			if err != nil {
				//return nil, err
				continue
			}
			var value map[string]*dynamodb.AttributeValue
			if err := json.Unmarshal(jsonValue, &value); err != nil {
				return nil, err
			}
			responses[tableName] = append(responses[tableName], value)
		}
	}
	return &dynamodb.BatchGetItemOutput{
		Responses: responses,
	}, nil
}
