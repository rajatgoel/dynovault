package itest

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/rajatgoel/dynovault/inmemory"
	"github.com/stretchr/testify/require"

	"github.com/rajatgoel/dynovault/handler"
)

func getDDBService(t *testing.T) *dynamodb.DynamoDB {
	ts := httptest.NewServer(handler.New(inmemory.New()))
	t.Cleanup(ts.Close)

	cfg := &aws.Config{
		Region:      aws.String("us-east-1"),
		Endpoint:    aws.String(ts.URL),
		MaxRetries:  aws.Int(0),
		Credentials: credentials.NewStaticCredentials("ID", "SECRET_KEY", "TOKEN"),
	}

	sess, err := session.NewSession(cfg)
	require.NoError(t, err)
	return dynamodb.New(sess, cfg)
}

func TestCreateTable(t *testing.T) {
	ddbSvc := getDDBService(t)

	testTableName := "TestTable"
	_, err := ddbSvc.CreateTable(&dynamodb.CreateTableInput{
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
	require.NoError(t, err)

	_, err = ddbSvc.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(testTableName),
	})
	require.NoError(t, err)
}

func TestInvalidCreateTable(t *testing.T) {
	ddbSvc := getDDBService(t)

	_, err := ddbSvc.CreateTable(&dynamodb.CreateTableInput{
		TableName: aws.String(""),
	})
	require.Error(t, err)
}

func TestListTables(t *testing.T) {
	ddbSvc := getDDBService(t)

	_, err := ddbSvc.ListTables(&dynamodb.ListTablesInput{
		Limit: aws.Int64(5),
	})
	require.NoError(t, err)
}

func TestDeleteTable(t *testing.T) {
	ddbSvc := getDDBService(t)

	_, err := ddbSvc.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: aws.String("TestTable"),
	})
	require.NoError(t, err)
}

func TestPutItem(t *testing.T) {
	ddbSvc := getDDBService(t)

	testTableName := "TestTable"
	_, err := ddbSvc.CreateTable(&dynamodb.CreateTableInput{
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
	require.NoError(t, err)

	_, err = ddbSvc.PutItem(&dynamodb.PutItemInput{
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
	require.NoError(t, err)
}

func TestGetItem(t *testing.T) {
	ddbSvc := getDDBService(t)

	testTableName := "TestTable"
	_, err := ddbSvc.CreateTable(&dynamodb.CreateTableInput{
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
	require.NoError(t, err)

	_, err = ddbSvc.PutItem(&dynamodb.PutItemInput{
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

	require.NoError(t, err)
	response, err := ddbSvc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(testTableName),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String("1"),
			},
		},
	})
	require.NotEmpty(t, response.Item)
	require.EqualValues(t, *response.Item["id"].S, "1")
	require.EqualValues(t, *response.Item["value"].S, "Test Value")
	require.NoError(t, err)
}

func TestDeleteItem(t *testing.T) {
	ddbSvc := getDDBService(t)

	_, err := ddbSvc.DeleteItem(&dynamodb.DeleteItemInput{
		TableName: aws.String("TestTable"),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String("1"),
			},
		},
	})
	require.NoError(t, err)
}

func TestBatchWrite(t *testing.T) {
	ddbSvc := getDDBService(t)

	testTableName := "TestTable"
	_, err := ddbSvc.CreateTable(&dynamodb.CreateTableInput{
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

	require.NoError(t, err)
	_, err = ddbSvc.BatchWriteItem(&dynamodb.BatchWriteItemInput{
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
	require.NoError(t, err)
}

func TestBatchGet(t *testing.T) {
	ddbSvc := getDDBService(t)

	testTableName := "TestTable"
	_, err := ddbSvc.CreateTable(&dynamodb.CreateTableInput{
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
	require.NoError(t, err)

	testValue := "Test Value"
	_, err = ddbSvc.BatchWriteItem(&dynamodb.BatchWriteItemInput{
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
	require.NoError(t, err)

	response, err := ddbSvc.BatchGetItem(&dynamodb.BatchGetItemInput{
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
	require.NoError(t, err)

	require.NotEmpty(t, response.Responses)
	responseItems := response.Responses[testTableName]
	require.NotEmpty(t, responseItems)
	testItem := map[string]*dynamodb.AttributeValue{}
	for _, item := range responseItems {
		if *item["id"].S == "1" {
			testItem = item
		}
	}
	require.NotNil(t, testItem)
	require.EqualValues(t, *testItem["value"].S, testValue)
}

func TestBatchDelete(t *testing.T) {
	ddbSvc := getDDBService(t)

	testTableName := "TestTable"
	_, err := ddbSvc.CreateTable(&dynamodb.CreateTableInput{
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
	require.NoError(t, err)

	_, err = ddbSvc.BatchWriteItem(&dynamodb.BatchWriteItemInput{
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
	require.NoError(t, err)

	_, err = ddbSvc.BatchWriteItem(&dynamodb.BatchWriteItemInput{
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
	require.NoError(t, err)

	response, err := ddbSvc.BatchGetItem(&dynamodb.BatchGetItemInput{
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
	require.NoError(t, err)
	require.Empty(t, response.Responses)
}
