package handler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func PutItem(ctx context.Context, s *state, input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	// Get the table key schema to determine which keys are pks
	describeTableOutput, err := DescribeTable(ctx, s, &dynamodb.DescribeTableInput{
		TableName: input.TableName,
	})
	if err != nil {
		return nil, err
	}
	key := *input.TableName
	for keyName, keyValue := range input.Item {
		for _, keySchemaElement := range describeTableOutput.Table.KeySchema {
			if keyName == *keySchemaElement.AttributeName {
				key = fmt.Sprintf("%s:%s-%s", key, keyName, *keyValue.S)
			}
		}
	}
	// Process the put
	// PutRequest.Item is map[string]*dynamodb.AttributeValue
	jsonValue, err := json.Marshal(input.Item)
	if err != nil {
		return nil, err
	}

	if err := s.kv.Put(ctx, []byte(key), jsonValue); err != nil {
		return nil, err
	}
	return &dynamodb.PutItemOutput{}, nil
}
