package feastle

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateRandomFeature(t *testing.T) {
	feature := GenerateRandomFeature()
	fmt.Printf("Feature: %s\n", feature)
	require.Equal(t, feature.EntityId, "todo")
}

func TestNewBatchWriteItemInput(t *testing.T) {
	feature := GenerateRandomFeature()
	feature1 := GenerateRandomFeature()
	input := NewBatchWriteItemInput([]*FeastFeature{feature, feature1})
	fmt.Printf("Feature as write input: %s\n", input)
	for tableName := range input.RequestItems {
		require.Equal(t, tableName, "todo")
	}
}
