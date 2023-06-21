// yaqpg_suite_test.go -- test suite rigging

package yaqpg_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/biztos/yaqpg"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/suite"
)

type TestLogger struct {
	Logged []string
}

func (t *TestLogger) Clear() {
	t.Logged = []string{}
}

func (t *TestLogger) Println(v ...interface{}) {
	t.Logged = append(t.Logged, fmt.Sprint(v...))
}

type DoesNotMarshal string

type YaqpgTestSuite struct {
	suite.Suite
	Queue  *yaqpg.Queue
	Logger *TestLogger
}

func (suite *YaqpgTestSuite) AssertLogged(msgs ...string) {

	assert := suite.Assert()

	// best comparison output is from EqualValues so just do that, however
	// it would be nice to have a regexp option too.
	for i, m := range msgs {
		msgs[i] = fmt.Sprintf("[%s] %s", suite.Queue.Name, m)
	}
	assert.EqualValues(msgs, suite.Logger.Logged, "log entries")

}

func (suite *YaqpgTestSuite) SetupSuite() {

	require := suite.Require()

	yaqpg.DefaultTableName = fmt.Sprintf("yaqpg_test_%s", ulid.Make())
	yaqpg.Now = func() time.Time {
		return time.Time{}
	}
	queue, err := yaqpg.StartDefaultQueue()
	require.NoError(err, "queue start err")
	require.NoError(queue.CreateSchema(), "schema err")
	suite.Logger = &TestLogger{}
	queue.Logger = suite.Logger
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

	// and the logger
	suite.Logger.Clear()

	// make sure any random fills we do have headroom
	yaqpg.FillBatchSize = 200

}

// The actual runner func:
func TestYaqpgTestSuite(t *testing.T) {
	suite.Run(t, new(YaqpgTestSuite))
}
