package feastle

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewBatchWriteItemInput(t *testing.T) {
	tableNames := []string{"table1", "table2"}
	feature := GenerateRandomFeature([]string{"table1"})
	feature1 := GenerateRandomFeature([]string{"table2"})
	feature2 := GenerateRandomFeature([]string{"table1"})
	input := NewBatchWriteItemInput([]*FeastFeature{feature, feature1, feature2})
	for tableName := range input.RequestItems {
		found := false
		for _, t := range tableNames {
			if t == tableName {
				found = true
			}
		}
		require.True(t, found)
	}
}

func TestGenerateRandomBatchWrite(t *testing.T) {
	tableNames := []string{"table1", "table2", "table3"}
	input := GenerateRandomBatchWrite(tableNames, 10)
	for tableName := range input.RequestItems {
		found := false
		for _, t := range tableNames {
			if t == tableName {
				found = true
			}
		}
		require.True(t, found)
	}
}
