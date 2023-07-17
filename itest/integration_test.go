package itest

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/require"

	"rajatgoel/dynovault/handler"
)

func getDDBService() (*dynamodb.DynamoDB, error) {
	ts := httptest.NewServer(handler.New())

	cfg := &aws.Config{
		Region:   aws.String("us-east-1"),
		Endpoint: aws.String(ts.URL),
		Credentials: credentials.NewStaticCredentials(
			"ID",
			"SECRET_KEY",
			"TOKEN",
		),
	}

	sess, err := session.NewSession(cfg)
	if err != nil {
		return nil, err
	}

	return dynamodb.New(sess, cfg), nil
}

func TestCreateTable(t *testing.T) {
	ddbSvc, err := getDDBService()
	require.NoError(t, err)

	var attributeDefinitions []*dynamodb.AttributeDefinition
	attributeDefinitions = append(attributeDefinitions, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("id"),
		AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
	})
	attributeDefinitions = append(attributeDefinitions, &dynamodb.AttributeDefinition{
		AttributeName: aws.String("value"),
		AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
	})

	var keySchema []*dynamodb.KeySchemaElement
	keySchema = append(keySchema, &dynamodb.KeySchemaElement{
		AttributeName: aws.String("id"),
		KeyType:       aws.String(dynamodb.KeyTypeHash),
	})

	_, err = ddbSvc.CreateTable(&dynamodb.CreateTableInput{
		TableName:            aws.String("TestTable"),
		AttributeDefinitions: attributeDefinitions,
		KeySchema:            keySchema,
	})
	require.NoError(t, err)
}

func TestListTables(t *testing.T) {
	ddbSvc, err := getDDBService()
	require.NoError(t, err)

	_, err = ddbSvc.ListTables(&dynamodb.ListTablesInput{
		Limit: aws.Int64(5),
	})
	require.NoError(t, err)
}

func TestDeleteTable(t *testing.T) {
	ddbSvc, err := getDDBService()
	require.NoError(t, err)

	_, err = ddbSvc.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: aws.String("TestTable"),
	})
	require.NoError(t, err)
}

func TestPutItem(t *testing.T) {
	ddbSvc, err := getDDBService()
	require.NoError(t, err)

	_, err = ddbSvc.PutItem(&dynamodb.PutItemInput{
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
	ddbSvc, err := getDDBService()
	require.NoError(t, err)

	_, err = ddbSvc.GetItem(&dynamodb.GetItemInput{
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
	ddbSvc, err := getDDBService()
	require.NoError(t, err)

	_, err = ddbSvc.UpdateItem(&dynamodb.UpdateItemInput{
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
	ddbSvc, err := getDDBService()
	require.NoError(t, err)

	_, err = ddbSvc.DeleteItem(&dynamodb.DeleteItemInput{
		TableName: aws.String("TestTable"),
		Key: map[string]*dynamodb.AttributeValue{
			"id": {
				S: aws.String("1"),
			},
		},
	})
	require.NoError(t, err)
}
