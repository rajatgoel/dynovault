package handler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func BatchWriteItem(
	ctx context.Context,
	s *state,
	input *dynamodb.BatchWriteItemInput,
) (*dynamodb.BatchWriteItemOutput, error) {
	fmt.Printf("BatchWriteItemInput: %s\n", input.String())
	for tableName, writeRequests := range input.RequestItems {
		for _, writeRequest := range writeRequests {
			key := tableName
			// A write request can contain delete XOR put
			// the AWS SDK should validate that for us
			if writeRequest.DeleteRequest != nil {
				for keyName, keyValue := range writeRequest.DeleteRequest.Key {
					key = fmt.Sprintf("%s:%s-%s", key, keyName, *keyValue.S)
				}
				if err := s.kv.Delete(ctx, []byte(key)); err != nil {
					return nil, err
				}
			} else {
				// Get the table key schema to determine which keys are pks
				describeTableOutput, err := DescribeTable(ctx, s, &dynamodb.DescribeTableInput{
					TableName: aws.String(tableName),
				})
				for keyName, keyValue := range writeRequest.PutRequest.Item {
					for _, keySchemaElement := range describeTableOutput.Table.KeySchema {
						if keyName == *keySchemaElement.AttributeName {
							key = fmt.Sprintf("%s:%s-%s", key, keyName, *keyValue.S)
						}
					}
				}
				// Process the put
				// PutRequest.Item is map[string]*dynamodb.AttributeValue
				jsonValue, err := json.Marshal(writeRequest.PutRequest.Item)
				if err != nil {
					return nil, err
				}

				if err := s.kv.Put(ctx, []byte(key), jsonValue); err != nil {
					return nil, err
				}
			}
		}
	}
	return &dynamodb.BatchWriteItemOutput{}, nil
}
