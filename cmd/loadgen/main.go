package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"log"
	"math/rand"
	"time"
)

var (
	endpointURL       string
	bulkfillDuration  time.Duration = 10 * time.Second
	numTables         int           = 5
	numParallelReader int           = 10
	numParallelWriter int           = 10
)

func main() {
	flag.StringVar(&endpointURL, "endpoint", "127.0.0.1:8779", "DynamoDB endpoint")
	flag.DurationVar(&bulkfillDuration, "bulkfill_duration", 10*time.Second, "Duration to run bulkfill for")
	flag.IntVar(&numTables, "num_tables", 10, "Number of tables to create")
	flag.IntVar(&numParallelReader, "num_parallel_reader", 10, "Number of parallel readers")
	flag.IntVar(&numParallelWriter, "num_parallel_writer", 10, "Number of parallel writers")

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

	lg := newLoadgen(
		loadgenParams{
			numTables:         numTables,
			numParallelReader: numParallelReader,
			numParallelWriter: numParallelWriter,
		},
		db,
	)

	err = lg.BulkUpload(context.Background(), bulkfillDuration)
	if err != nil {
		panic(fmt.Errorf("failed to bulk upload: %s", err))
	}

	err = lg.Run(context.Background())
	if err != nil {
		panic(fmt.Errorf("failed to run loadgen: %s", err))
	}

}

type loadgenParams struct {
	numTables         int
	numParallelReader int
	numParallelWriter int
}

type loadgen struct {
	param  loadgenParams
	db     *dynamodb.DynamoDB
	tables []string
}

func newLoadgen(param loadgenParams, db *dynamodb.DynamoDB) *loadgen {
	return &loadgen{
		param: param,
		db:    db,
	}
}

func (l *loadgen) BulkUpload(ctx context.Context, duration time.Duration) error {
	tables := generateTableNames(l.param.numTables)
	log.Println("Creating tables.... len(tables):", len(tables))

	for _, tableName := range tables {
		output, err := l.db.CreateTable(&dynamodb.CreateTableInput{
			TableName: aws.String(tableName),
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String("entity_id"),
					AttributeType: aws.String("S"),
				},
			},
			// BillingMode: "PAY_PER_REQUEST",
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("entity_id"),
					KeyType:       aws.String("HASH"),
				},
			},
		})
		if err != nil {
			log.Println("Failed to create table", tableName, err)
			return err
		}

		log.Println("Created table", tableName, output)
	}

	for _, tableName := range tables {
		table, err := l.db.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: aws.String(tableName),
		})
		if err != nil {
			return err
		}

		log.Println("Table description", tableName, table)
	}

	return nil
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
	doBatchWriteItem,
	// doCreateTable,
	// doDeleteItem,
	// doDeleteTable,
	// doDescribeTable,
	// doGetItem,
	// doPutItem,
}

func doBatchGetItem(ctx context.Context, db *dynamodb.DynamoDB) error {
	output, err := db.BatchGetItem(&dynamodb.BatchGetItemInput{
		RequestItems: map[string]*dynamodb.KeysAndAttributes{
			"test": {
				Keys: []map[string]*dynamodb.AttributeValue{
					{
						"id": {
							S: aws.String("1"),
						},
					},
				},
			},
		},
	})
	fmt.Println(output, err)
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

func generateTableNames(numTables int) []string {
	tableNames := make([]string, 0, numTables)
	for i := 0; i < numTables; i++ {
		tableNames = append(tableNames, fmt.Sprintf("feastle.driver_hourly_stats.%d", i))

	}
	return tableNames
}
