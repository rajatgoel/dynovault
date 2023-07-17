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

	cfg := aws.NewConfig()
	cfg = cfg.WithRegion("us-east-1")
	cfg = cfg.WithEndpoint(ts.URL)
	cfg = cfg.WithCredentials(credentials.NewStaticCredentials(
		"ID",
		"SECRET_KEY",
		"TOKEN",
	))

	sess, err := session.NewSession(cfg)
	if err != nil {
		return nil, err
	}

	return dynamodb.New(sess, cfg), nil
}

func TestListTables(t *testing.T) {
	ddbSvc, err := getDDBService()
	require.NoError(t, err)

	// Build the request with its input parameters
	_, err = ddbSvc.ListTables(&dynamodb.ListTablesInput{
		Limit: aws.Int64(5),
	})
	require.NoError(t, err)
}

func TestCreateTable(t *testing.T) {
	ddbSvc, err := getDDBService()
	require.NoError(t, err)

	tableName := "TestTable"
	attributeName := "TestAttribute"
	attributeType := dynamodb.ScalarAttributeTypeS
	keyType := dynamodb.KeyTypeHash

	var attributeDefinitions []*dynamodb.AttributeDefinition
	attributeDefinitions = append(attributeDefinitions, &dynamodb.AttributeDefinition{
		AttributeName: &attributeName,
		AttributeType: &attributeType,
	})

	var keySchema []*dynamodb.KeySchemaElement
	keySchema = append(keySchema, &dynamodb.KeySchemaElement{
		AttributeName: &attributeName,
		KeyType:       &keyType,
	})

	_, err = ddbSvc.CreateTable(&dynamodb.CreateTableInput{
		TableName:            &tableName,
		AttributeDefinitions: attributeDefinitions,
		KeySchema:            keySchema,
	})
	require.NoError(t, err)
}
