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

	_, err := ddbSvc.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String("TestTable"),
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

	_, err := ddbSvc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String("TestTable"),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String("1"),
			},
		},
	})
	require.NoError(t, err)
}

func TestUpdateItem(t *testing.T) {
	ddbSvc := getDDBService(t)

	_, err := ddbSvc.UpdateItem(&dynamodb.UpdateItemInput{
		TableName: aws.String("TestTable"),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String("1"),
			},
		},
		UpdateExpression: aws.String("SET #V = :v"),
		ExpressionAttributeNames: map[string]*string{
			"#V": aws.String("value"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":v": {
				S: aws.String("Test Value Updated"),
			},
		},
		ReturnValues: aws.String("ALL_NEW"),
	})
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

	_, err := ddbSvc.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			"TestTable": []*dynamodb.WriteRequest{
				&dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"id": &dynamodb.AttributeValue{S: aws.String("1")},
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

	_, err := ddbSvc.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			"TestTable": []*dynamodb.WriteRequest{
				&dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]*dynamodb.AttributeValue{
							"id": &dynamodb.AttributeValue{S: aws.String("1")},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = ddbSvc.BatchGetItem(&dynamodb.BatchGetItemInput{
		RequestItems: map[string]*dynamodb.KeysAndAttributes{
			"TestTable": &dynamodb.KeysAndAttributes{
				ProjectionExpression: aws.String("id"),
				Keys: []map[string]*dynamodb.AttributeValue{
					map[string]*dynamodb.AttributeValue{
						"id": &dynamodb.AttributeValue{S: aws.String("1")},
					},
				},
			},
		},
	})
	require.NoError(t, err)
}
