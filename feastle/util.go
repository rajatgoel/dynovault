package feastle

import (
	"fmt"
	"math/rand"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type FeastFeature struct {
	FeatureName    string
	EntityId       string
	EventTimestamp string
	Values         map[string][]byte
}

func GenerateRandomFeature(featureNames []string) *FeastFeature {
	featureName := featureNames[rand.Intn(len(featureNames))]
	randId := fmt.Sprintf("key-%d", rand.Int()%1000)
	randTs := fmt.Sprintf("ts-%d", rand.Int()%1000)
	randValue1 := fmt.Sprintf("%d", rand.Int()%100000)
	randValue2 := fmt.Sprintf("%d", rand.Int()%100000)
	randValue3 := fmt.Sprintf("%d", rand.Int()%100000)

	return &FeastFeature{
		FeatureName:    featureName,
		EntityId:       randId,
		EventTimestamp: randTs,
		Values: map[string][]byte{
			"key1": []byte(randValue1),
			"key2": []byte(randValue2),
			"key3": []byte(randValue3),
		},
	}
}

func (f *FeastFeature) ddbItem() map[string]*dynamodb.AttributeValue {
	values := map[string]*dynamodb.AttributeValue{}
	for k, v := range f.Values {
		values[k] = &dynamodb.AttributeValue{B: v}
	}
	item := map[string]*dynamodb.AttributeValue{
		"entity_id": &dynamodb.AttributeValue{S: aws.String(f.EntityId)},
		"event_ts":  &dynamodb.AttributeValue{S: aws.String(f.EventTimestamp)},
		"values":    &dynamodb.AttributeValue{M: values},
	}
	return item
}

func NewBatchWriteItemInput(features []*FeastFeature) *dynamodb.BatchWriteItemInput {
	requestItems := map[string][]*dynamodb.WriteRequest{}
	for _, f := range features {
		requestItems[f.FeatureName] = append(requestItems[f.FeatureName], &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: f.ddbItem(),
			},
		})
	}
	return &dynamodb.BatchWriteItemInput{
		RequestItems: requestItems,
	}
}

func NewBatchGetItemInput(features []*FeastFeature) *dynamodb.BatchGetItemInput {
	requestItems := map[string]*dynamodb.KeysAndAttributes{}
	for _, f := range features {
		requestItems[f.FeatureName] = &dynamodb.KeysAndAttributes{
			Keys: []map[string]*dynamodb.AttributeValue{
				{
					"entity_id": &dynamodb.AttributeValue{S: aws.String(f.EntityId)},
					"event_ts":  &dynamodb.AttributeValue{S: aws.String(f.EventTimestamp)},
				},
			},
		}
	}

	return &dynamodb.BatchGetItemInput{
		RequestItems: requestItems,
	}
}

func GenerateRandomBatchWrite(tables []string, batchSize int) *dynamodb.BatchWriteItemInput {
	randomFeatures := []*FeastFeature{}
	for i := 0; i < batchSize; i++ {
		randomFeatures = append(randomFeatures, GenerateRandomFeature(tables))
	}
	return NewBatchWriteItemInput(randomFeatures)
}
