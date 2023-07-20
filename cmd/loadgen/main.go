package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/rajatgoel/dynovault/feastle"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

var (
	endpointURL       string
	bulkfillDuration  time.Duration
	numTables         int
	numParallelReader int
	numParallelWriter int
)

func main() {
	flag.StringVar(&endpointURL, "endpoint", "http://127.0.0.1:8779", "DynamoDB endpoint")
	flag.DurationVar(&bulkfillDuration, "bulkfill_duration", 1*time.Second, "Duration to run bulkfill for")
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

	ctx, cancel := context.WithCancel(context.Background())
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		cancel()
	}()

	err = lg.BulkUpload(ctx, bulkfillDuration)
	if err != nil {
		panic(fmt.Errorf("failed to bulk upload: %s", err))
	}

	err = lg.Run(ctx)
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

	writtenCount int
	readCount    int
}

func newLoadgen(param loadgenParams, db *dynamodb.DynamoDB) *loadgen {
	return &loadgen{
		param: param,
		db:    db,
	}
}

func (l *loadgen) createTables(ctx context.Context, tables []string) error {
	beginTime := time.Now()
	for _, tableName := range tables {
		_, err := l.db.CreateTable(&dynamodb.CreateTableInput{
			TableName: aws.String(tableName),
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String("entity_id"),
					AttributeType: aws.String("S"),
				},
			},
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
	}

	log.Println("Created tables in...", time.Since(beginTime))

	for _, tableName := range tables {
		table, err := l.db.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: aws.String(tableName),
		})
		if err != nil {
			return err
		}

		if *table.Table.TableName != tableName {
			panic(fmt.Errorf("table name mismatch... %s %s", *table.Table.TableName, tableName))
		}
	}

	return nil
}

func (l *loadgen) BulkUpload(ctx context.Context, duration time.Duration) error {
	tables := generateTableNames(l.param.numTables)
	log.Println("Creating tables.... len(tables):", len(tables))

	err := l.createTables(ctx, tables)
	if err != nil {
		return err
	}

	l.tables = tables

	wg := sync.WaitGroup{}
	wg.Add(l.param.numParallelWriter)
	var writerErr error
	for i := 0; i < l.param.numParallelWriter; i++ {
		go func() {
			defer wg.Done()

			timer := time.NewTimer(duration)
			defer timer.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-timer.C:
					return
				default:
				}
				err := l.doBatchWriteItem(ctx)
				if err != nil {
					writerErr = err
				}

			}
		}()
	}
	wg.Wait()

	return writerErr
}

// Run blocks until the context is cancelled.
func (l *loadgen) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 1% write, 99% get
		switch rand.Intn(100) {
		case 0:
			if err := l.doBatchWriteItem(ctx); err != nil {
				fmt.Printf("failed to write: %v", err)
			}
		default:
			if err := l.doBatchGetItem(ctx); err != nil {
				fmt.Printf("failed to get: %v", err)
			}
		}
	}
}

func (l *loadgen) doBatchGetItem(ctx context.Context) error {
	numFeatures := rand.Int()%100 + 1
	features := make([]*feastle.FeastFeature, numFeatures)
	for i := 0; i < numFeatures; i++ {
		features[i] = feastle.GenerateRandomFeature(l.tables)
	}
	batchGetItemInput := feastle.NewBatchGetItemInput(features)
	out, err := l.db.BatchGetItem(batchGetItemInput)
	if err != nil {
		return err
	}
	log.Println("BatchGetItem output:", out)
	return nil
}

func (l *loadgen) doBatchWriteItem(ctx context.Context) error {
	numFeatures := rand.Int()%100 + 1
	features := make([]*feastle.FeastFeature, numFeatures)
	for i := 0; i < numFeatures; i++ {
		features[i] = feastle.GenerateRandomFeature(l.tables)
	}
	batchWriteItemInput := feastle.NewBatchWriteItemInput(features)
	out, err := l.db.BatchWriteItem(batchWriteItemInput)
	if err != nil {
		return err
	}
	log.Println("BatchWriteItem output:", out)
	return nil
}

func (l *loadgen) doDeleteItem(ctx context.Context) error {
	deleteItemInput := &dynamodb.DeleteItemInput{}

	_, err := l.db.DeleteItem(deleteItemInput)
	if err != nil {
		return err
	}
	return nil
}

func (l *loadgen) doDeleteTable(ctx context.Context) error {
	// todo
	return nil
}

func generateTableNames(numTables int) []string {
	tableNames := make([]string, 0, numTables)
	for i := 0; i < numTables; i++ {
		tableNames = append(tableNames, fmt.Sprintf("feastle.driver_hourly_stats.%d", i))
	}
	return tableNames
}
