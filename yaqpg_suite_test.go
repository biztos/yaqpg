// yaqpg_suite_test.go -- test suite rigging

package yaqpg_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/biztos/yaqpg"

	"github.com/jackc/pgx/v5"
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
	Queue               *yaqpg.Queue
	Logger              *TestLogger
	Database            string
	OriginalDatabaseUrl string
	KeepDatabase        bool
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

	// It's a pain running tests against even a dev database, because we have
	// to zero out the data all the time, and it's not realistic to have the
	// developer always remember to set the correct database, so we override
	// that here and create a new test database.
	//
	// Doing this via DATABASE_URL is messy but seems like the only way that
	// will (should) work.
	dbname := strings.ToLower(fmt.Sprintf("yaqpg_test_%s", ulid.Make()))
	orig_dburl := os.Getenv("DATABASE_URL")
	if orig_dburl == "" {
		// If you forgot to set it, we assume (dangerously?) the default.
		orig_dburl = "postgresql://localhost:5432/"
	}
	u, err := url.Parse(orig_dburl)
	require.NoError(err, "parse env DATABASE_URL")
	u.Path = "/" + dbname
	test_dburl := u.String()

	// Now we can create that database, with a connection from the existing
	// one.  For this we don't use a pool because we just want the one exec.
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, orig_dburl)
	require.NoError(err, "connect to database for create")
	sql := fmt.Sprintf("CREATE DATABASE %s TEMPLATE template0;", dbname)
	_, err = conn.Exec(ctx, sql)
	conn.Close(ctx)
	require.NoError(err, "exec create database")

	// All further operations are on this new, test database.
	suite.Database = dbname
	suite.OriginalDatabaseUrl = orig_dburl
	os.Setenv("DATABASE_URL", test_dburl)

	// And we may wish to keep it...
	keep := os.Getenv("KEEP_DB")
	if keep != "" && keep != "0" && keep != "false" {
		suite.KeepDatabase = true
	}

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

	// We can't drop the database if it has an open pool.
	suite.Queue.Pool.Close()

	if suite.KeepDatabase {
		fmt.Println("Keeping database:", suite.Database)
		return
	}

	// In order to drop the test database we need a connection to the original,
	// which we have fortunately kept!
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, suite.OriginalDatabaseUrl)
	require.NoError(err, "connect to original database")
	sql := fmt.Sprintf("DROP DATABASE %s;", suite.Database)
	_, err = conn.Exec(ctx, sql)
	defer conn.Close(ctx)
	require.NoError(err, "exec drop database")

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
