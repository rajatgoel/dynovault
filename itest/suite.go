package itest

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type Suite struct {
	suite.Suite

	db *dynamodb.DynamoDB
}

func New(t *testing.T, url string) *Suite {
	cfg := &aws.Config{
		Region:      aws.String("us-east-1"),
		Endpoint:    aws.String(url),
		MaxRetries:  aws.Int(0),
		Credentials: credentials.NewStaticCredentials("ID", "SECRET_KEY", "TOKEN"),
	}

	sess, err := session.NewSession(cfg)
	require.NoError(t, err)
	db := dynamodb.New(sess, cfg)
	return &Suite{db: db}
}

func (s *Suite) TestCreateTable() {
	testTableName := "TestTable"
	_, err := s.db.CreateTable(&dynamodb.CreateTableInput{
		TableName: aws.String(testTableName),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
			{
				AttributeName: aws.String("value"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       aws.String(dynamodb.KeyTypeHash),
			},
		},
	})
	require.NoError(s.T(), err)

	_, err = s.db.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(testTableName),
	})
	require.NoError(s.T(), err)
}

func (s *Suite) TestInvalidCreateTable() {
	_, err := s.db.CreateTable(&dynamodb.CreateTableInput{
		TableName: aws.String(""),
	})
	require.Error(s.T(), err)
}

func (s *Suite) TestDeleteTable() {
	testTableName := "TestTable"
	_, err := s.db.CreateTable(&dynamodb.CreateTableInput{
		TableName: aws.String(testTableName),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
			{
				AttributeName: aws.String("value"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       aws.String(dynamodb.KeyTypeHash),
			},
		},
	})
	require.NoError(s.T(), err)

	response, err := s.db.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: aws.String(testTableName),
	})
	require.NoError(s.T(), err)
	require.NotNil(s.T(), response.TableDescription)
	require.Equal(s.T(), *response.TableDescription.TableName, testTableName)

	_, err = s.db.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(testTableName),
	})
	require.Error(s.T(), err)
}

func (s *Suite) TestPutItem() {
	testTableName := "TestTable"
	_, err := s.db.CreateTable(&dynamodb.CreateTableInput{
		TableName: aws.String(testTableName),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
			{
				AttributeName: aws.String("value"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       aws.String(dynamodb.KeyTypeHash),
			},
		},
	})
	require.NoError(s.T(), err)

	_, err = s.db.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(testTableName),
		Item: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String("1"),
			},
			"value": {
				S: aws.String("Test Value"),
			},
		},
	})
	require.NoError(s.T(), err)
}

func (s *Suite) TestGetItem() {
	testTableName := "TestTable"
	_, err := s.db.CreateTable(&dynamodb.CreateTableInput{
		TableName: aws.String(testTableName),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
			{
				AttributeName: aws.String("value"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       aws.String(dynamodb.KeyTypeHash),
			},
		},
	})
	require.NoError(s.T(), err)

	_, err = s.db.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(testTableName),
		Item: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String("1"),
			},
			"value": {
				S: aws.String("Test Value"),
			},
		},
	})

	require.NoError(s.T(), err)
	response, err := s.db.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(testTableName),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String("1"),
			},
		},
	})
	require.NotEmpty(s.T(), response.Item)
	require.EqualValues(s.T(), *response.Item["id"].S, "1")
	require.EqualValues(s.T(), *response.Item["value"].S, "Test Value")
	require.NoError(s.T(), err)
}

func (s *Suite) TestDeleteItem() {
	testTableName := "TestTable"
	_, err := s.db.CreateTable(&dynamodb.CreateTableInput{
		TableName: aws.String(testTableName),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
			{
				AttributeName: aws.String("value"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       aws.String(dynamodb.KeyTypeHash),
			},
		},
	})
	require.NoError(s.T(), err)

	_, err = s.db.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(testTableName),
		Item: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String("1"),
			},
			"value": {
				S: aws.String("Test Value"),
			},
		},
	})
	require.NoError(s.T(), err)

	response, err := s.db.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(testTableName),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String("1"),
			},
		},
	})
	require.NotEmpty(s.T(), response.Item)
	require.EqualValues(s.T(), *response.Item["id"].S, "1")
	require.EqualValues(s.T(), *response.Item["value"].S, "Test Value")
	require.NoError(s.T(), err)

	_, err = s.db.DeleteItem(&dynamodb.DeleteItemInput{
		TableName: aws.String("TestTable"),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String("1"),
			},
		},
	})
	require.NoError(s.T(), err)

	response, err = s.db.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(testTableName),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String("1"),
			},
		},
	})
	require.Empty(s.T(), response.Item)
	require.NoError(s.T(), err)
}

func (s *Suite) TestBatchWrite() {
	testTableName := "TestTable"
	_, err := s.db.CreateTable(&dynamodb.CreateTableInput{
		TableName: aws.String(testTableName),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
			{
				AttributeName: aws.String("value"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       aws.String(dynamodb.KeyTypeHash),
			},
		},
	})

	require.NoError(s.T(), err)
	_, err = s.db.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			testTableName: {
				{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"id":    {S: aws.String("1")},
							"value": {S: aws.String("test value")},
						},
					},
				},
			},
		},
	})
	require.NoError(s.T(), err)
}

func (s *Suite) TestBatchGet() {
	testTableName := "TestTable"
	_, err := s.db.CreateTable(&dynamodb.CreateTableInput{
		TableName: aws.String(testTableName),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
			{
				AttributeName: aws.String("value"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       aws.String(dynamodb.KeyTypeHash),
			},
		},
	})
	require.NoError(s.T(), err)

	testValue := "Test Value"
	_, err = s.db.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			testTableName: {
				{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"id":    {S: aws.String("1")},
							"value": {S: aws.String(testValue)},
						},
					},
				},
			},
		},
	})
	require.NoError(s.T(), err)

	response, err := s.db.BatchGetItem(&dynamodb.BatchGetItemInput{
		RequestItems: map[string]*dynamodb.KeysAndAttributes{
			testTableName: {
				ProjectionExpression: aws.String("id"),
				Keys: []map[string]*dynamodb.AttributeValue{
					{
						"id": {S: aws.String("1")},
					},
				},
			},
		},
	})
	require.NoError(s.T(), err)
	require.NotEmpty(s.T(), response.Responses)

	responseItems := response.Responses[testTableName]
	require.NotEmpty(s.T(), responseItems)
	testItem := map[string]*dynamodb.AttributeValue{}
	for _, item := range responseItems {
		if *item["id"].S == "1" {
			testItem = item
		}
	}
	require.NotNil(s.T(), testItem)
	require.EqualValues(s.T(), *testItem["value"].S, testValue)
}

func (s *Suite) TestBatchDelete() {
	testTableName := "TestTable"
	_, err := s.db.CreateTable(&dynamodb.CreateTableInput{
		TableName: aws.String(testTableName),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
			{
				AttributeName: aws.String("value"),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       aws.String(dynamodb.KeyTypeHash),
			},
		},
	})
	require.NoError(s.T(), err)

	_, err = s.db.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			testTableName: {
				{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"id": {S: aws.String("1")},
						},
					},
				},
			},
		},
	})
	require.NoError(s.T(), err)

	_, err = s.db.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			testTableName: {
				{
					DeleteRequest: &dynamodb.DeleteRequest{
						Key: map[string]*dynamodb.AttributeValue{
							"id": {S: aws.String("1")},
						},
					},
				},
			},
		},
	})
	require.NoError(s.T(), err)

	response, err := s.db.BatchGetItem(&dynamodb.BatchGetItemInput{
		RequestItems: map[string]*dynamodb.KeysAndAttributes{
			testTableName: {
				ProjectionExpression: aws.String("id"),
				Keys: []map[string]*dynamodb.AttributeValue{
					{
						"id": {S: aws.String("1")},
					},
				},
			},
		},
	})
	require.NoError(s.T(), err)
	require.Empty(s.T(), response.Responses)
}
