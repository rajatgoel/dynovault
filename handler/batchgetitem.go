package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func BatchGetItem(
	ctx context.Context,
	s *state,
	input *dynamodb.BatchGetItemInput,
) (*dynamodb.BatchGetItemOutput, error) {
	responses := map[string][]map[string]*dynamodb.AttributeValue{}

	for tableName, requestItem := range input.RequestItems {
		for _, expression := range strings.Split(*requestItem.ProjectionExpression, ",") {
			// Assume the projection expression is just a comma separated list
			// of name of the attribute names being retrieved
			// TODO: expand to more types of expressions later
			attributeName := strings.TrimSpace(expression)
			key := fmt.Sprintf("%s:%s", tableName, attributeName)
			jsonValue, err := s.kv.Get(ctx, []byte(key))
			if err != nil {
				return nil, err
			}
			var value dynamodb.AttributeValue
			if err := json.Unmarshal(jsonValue, &value); err != nil {
				return nil, err
			}
			attribute := map[string]*dynamodb.AttributeValue{
				attributeName: &value,
			}
			responses[tableName] = append(responses[tableName], attribute)
		}
	}
	fmt.Println(responses)
	return &dynamodb.BatchGetItemOutput{
		Responses: responses,
	}, nil
}
