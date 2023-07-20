package feastle

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateRandomFeature(t *testing.T) {
	feature := GenerateRandomFeature()
	fmt.Printf("Feature %s\n", feature)
	require.Equal(t, feature.EntityId, "todo")
}
