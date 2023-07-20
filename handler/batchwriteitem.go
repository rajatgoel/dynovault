package handler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func getPartitionKey(ctx context.Context, s *state, tableName string) (string, error) {
	key, found := s.partitionKey.Load(tableName)
	if found {
		return key.(string), nil
	}

	// Get the table key schema to determine which keys are pks
	describeTableOutput, err := DescribeTable(ctx, s, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return "", err
	}

	hashKey := ""
	for _, keySchemaElement := range describeTableOutput.Table.KeySchema {
		if *keySchemaElement.KeyType == "HASH" {
			hashKey = *keySchemaElement.AttributeName
			break
		}
	}
	s.partitionKey.Store(tableName, hashKey)
	return hashKey, nil
}

func BatchWriteItem(
	ctx context.Context,
	s *state,
	input *dynamodb.BatchWriteItemInput,
) (*dynamodb.BatchWriteItemOutput, error) {
	for tableName, writeRequests := range input.RequestItems {
		partitionKey, err := getPartitionKey(ctx, s, tableName)
		if err != nil {
			return nil, err
		}

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
				key = fmt.Sprintf("%s:%s-%s", key, partitionKey, *writeRequest.PutRequest.Item[partitionKey].S)
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
	return &dynamodb.BatchWriteItemOutput{
		UnprocessedItems: map[string][]*dynamodb.WriteRequest{},
	}, nil
}
