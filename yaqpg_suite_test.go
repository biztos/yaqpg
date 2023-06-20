// yaqpg_suite_test.go -- test suite rigging

package yaqpg_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/biztos/yaqpg"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/suite"
)

type DoesNotMarshal string

type YaqpgTestSuite struct {
	suite.Suite
	Queue *yaqpg.Queue
}

func (suite *YaqpgTestSuite) SetupSuite() {

	require := suite.Require()

	yaqpg.DefaultTableName = fmt.Sprintf("yaqpg_test_%s", ulid.Make())

	queue, err := yaqpg.StartDefaultQueue()
	require.NoError(err, "queue start err")
	require.NoError(queue.CreateSchema(), "schema err")
	suite.Queue = queue

}

func (suite *YaqpgTestSuite) TearDownSuite() {

	require := suite.Require()

	val := os.Getenv("KEEP_DB")
	if val != "" && val != "0" && val != "false" {
		return
	}

	require.NotNil(suite.Queue, "queue")
	require.NotNil(suite.Queue.Pool, "pool")

	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s;", suite.Queue.TableName)
	_, err := suite.Queue.Pool.Exec(context.Background(), sql)
	require.NoError(err, "drop exec")

}

func (suite *YaqpgTestSuite) SetupTest() {

	require := suite.Require()

	// zero out the queue
	sql := fmt.Sprintf("TRUNCATE TABLE %s;", suite.Queue.TableName)
	_, err := suite.Queue.Pool.Exec(context.Background(), sql)
	require.NoError(err, "truncate exec")

}

// The actual runner func:
func TestYaqpgTestSuite(t *testing.T) {
	suite.Run(t, new(YaqpgTestSuite))
}
