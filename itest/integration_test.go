package itest

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"net/http/httptest"
	handler "rajatgoel/dynovault/handler"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/require"
)

func TestListTables(t *testing.T) {
	ts := httptest.NewServer(handler.New())

	cfg := aws.NewConfig().WithRegion("us-east-1").WithEndpoint(ts.URL)
	sess, err := session.NewSession(cfg)
	require.NoError(t, err)

	ddbSvc := dynamodb.New(sess, cfg)

	// Build the request with its input parameters
	_, err = ddbSvc.ListTables(&dynamodb.ListTablesInput{
		Limit: aws.Int64(5),
	})
	require.NoError(t, err)
}
