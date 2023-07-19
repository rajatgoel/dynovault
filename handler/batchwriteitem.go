package handler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func BatchWriteItem(
	ctx context.Context,
	s *state,
	input *dynamodb.BatchWriteItemInput,
) (*dynamodb.BatchWriteItemOutput, error) {
	for tableName, writeRequests := range input.RequestItems {
		for _, writeRequest := range writeRequests {
			// A write request can contain delete XOR put
			// the AWS SDK should validate that for us
			if writeRequest.DeleteRequest != nil {
				for attributeName, _ := range writeRequest.DeleteRequest.Key {
					// Process the delete, not checking the primary keys
					key := fmt.Sprintf("%s:%s", tableName, attributeName)
					if err := s.kv.Delete(ctx, []byte(key)); err != nil {
						return nil, err
					}
				}
			} else {
				for attributeName, attributeValue := range writeRequest.PutRequest.Item {
					// Process the put
					jsonValue, err := json.Marshal(attributeValue)
					if err != nil {
						return nil, err
					}

					key := fmt.Sprintf("%s:%s", tableName, attributeName)
					if err := s.kv.Put(ctx, []byte(key), jsonValue); err != nil {
						return nil, err
					}
				}
			}
		}
	}

	return &dynamodb.BatchWriteItemOutput{}, nil
}
