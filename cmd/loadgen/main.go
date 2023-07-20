package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"math/rand"
	"time"
)

var (
	endpointURL string
)

func main() {
	flag.StringVar(&endpointURL, "endpoint", "127.0.0.1:8779", "DynamoDB endpoint")
	flag.Parse()
	cfg := &aws.Config{
		Region:      aws.String("us-east-1"),
		Endpoint:    aws.String(endpointURL),
		MaxRetries:  aws.Int(0),
		Credentials: credentials.NewStaticCredentials("ID", "SECRET_KEY", "TOKEN"),
	}

	sess, err := session.NewSession(cfg)
	if err != nil {
		panic(fmt.Errorf("failed to create new session: %s", err))
	}
	db := dynamodb.New(sess, cfg)

	lg := loadgen{db: db}
	err = lg.Run(context.Background())
	if err != nil {
		panic(fmt.Errorf("failed to run loadgen: %s", err))
	}
}

type loadgen struct {
	db *dynamodb.DynamoDB
}

// Run blocks until the context is cancelled.
func (l *loadgen) Run(ctx context.Context) error {

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		action := actions[rand.Int()%len(actions)]
		if err := action(ctx, l.db); err != nil {
			fmt.Printf("failed to execute action: %s", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

var actions = []func(context.Context, *dynamodb.DynamoDB) error{
	doBatchGetItem,
	// doBatchWriteItem,
	// doCreateTable,
	// doDeleteItem,
	// doDeleteTable,
	// doDescribeTable,
	// doGetItem,
	// doPutItem,
}

func doBatchGetItem(ctx context.Context, db *dynamodb.DynamoDB) error {
	fmt.Printf("doBatchGetItem\n")
	return nil
}

func doBatchWriteItem(ctx context.Context, db *dynamodb.DynamoDB) error {
	return nil
}

func doCreateTable(ctx context.Context, db *dynamodb.DynamoDB) error {
	return nil
}

func doDeleteItem(ctx context.Context, db *dynamodb.DynamoDB) error {
	return nil
}

func doDeleteTable(ctx context.Context, db *dynamodb.DynamoDB) error {
	return nil
}

func doDescribeTable(ctx context.Context, db *dynamodb.DynamoDB) error {
	return nil
}

func doGetItem(ctx context.Context, db *dynamodb.DynamoDB) error {
	return nil
}

func doPutItem(ctx context.Context, db *dynamodb.DynamoDB) error {
	return nil
}
