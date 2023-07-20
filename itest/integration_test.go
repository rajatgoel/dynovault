package itest

import (
	"net/http/httptest"
	"testing"

	"github.com/rajatgoel/dynovault/inmemory"
	"github.com/stretchr/testify/suite"

	"github.com/rajatgoel/dynovault/handler"
)

func TestInMemory(t *testing.T) {
	ts := httptest.NewServer(handler.New(inmemory.New()))
	t.Cleanup(ts.Close)

	suite.Run(t, New(t, ts.URL))
}
