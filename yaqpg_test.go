// yaqpg_test.go

package yaqpg_test

import (
	"testing"

	"github.com/biztos/yaqpg"

	"github.com/stretchr/testify/assert"
)

func TestPlaceholder(t *testing.T) {
	assert := assert.New(t)
	assert.True(true, "truthy placeholder")
	assert.Equal("jobs", yaqpg.DefaultQueueName)
}
