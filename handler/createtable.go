package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/lithammer/shortuuid/v4"
)

func CreateTable(ctx context.Context, s *state, input *dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error) {
	// TODO: Check if table already exists

	now := time.Now()
	key := fmt.Sprintf("$table:%s", *input.TableName)
	value := &dynamodb.TableDescription{
		TableId:              aws.String(shortuuid.New()),
		TableName:            input.TableName,
		TableStatus:          aws.String("ACTIVE"),
		CreationDateTime:     aws.Time(now),
		AttributeDefinitions: input.AttributeDefinitions,
		KeySchema:            input.KeySchema,
		TableClassSummary: &dynamodb.TableClassSummary{
			LastUpdateDateTime: aws.Time(now),
			TableClass:         aws.String("STANDARD"),
		},
	}

	jsonValue, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}

	if err := s.kv.Put(ctx, []byte(key), jsonValue); err != nil {
		return nil, err
	}

	return &dynamodb.CreateTableOutput{
		TableDescription: value,
	}, nil
}
