// yaqpg_test.go

package yaqpg_test

import (
	"os"
	"time"

	"github.com/biztos/yaqpg"
)

func (suite *YaqpgTestSuite) TestCountReadyError() {

	require := suite.Require()

	orig := suite.Queue.TableName
	defer func() { suite.Queue.TableName = orig }()
	suite.Queue.TableName = "syntax/error/right/here"

	_, err := suite.Queue.CountReady()
	require.ErrorContains(err, "SQLSTATE 42601")
}

func (suite *YaqpgTestSuite) TestCountReadyOK() {

	require := suite.Require()
	require.NoError(suite.Queue.Fill(3, 4, 1))
	c, err := suite.Queue.CountReady()
	require.NoError(err)
	require.Equal(3, c, "number ready")
}

func (suite *YaqpgTestSuite) TestCountPendingError() {

	require := suite.Require()

	orig := suite.Queue.TableName
	defer func() { suite.Queue.TableName = orig }()
	suite.Queue.TableName = "syntax/error/right/here"

	_, err := suite.Queue.CountPending()
	require.ErrorContains(err, "SQLSTATE 42601")
}

func (suite *YaqpgTestSuite) TestCountPendingOK() {

	require := suite.Require()
	require.NoError(suite.Queue.Fill(3, 4, time.Minute))
	c, err := suite.Queue.CountPending()
	require.NoError(err)
	require.Equal(3, c, "number Pending")
}

func (suite *YaqpgTestSuite) TestMustStartNamedQueuePanics() {

	require := suite.Require()

	orig := os.Getenv("DATABASE_URL")
	defer os.Setenv("DATABASE_URL", orig)
	os.Setenv("DATABASE_URL", "")

	require.Panics(func() { yaqpg.MustStartNamedQueue("whatever") })

}

func (suite *YaqpgTestSuite) TestMustStartNamedQueueOK() {

	require := suite.Require()
	require.NotPanics(func() { yaqpg.MustStartNamedQueue("whatever") })

}

func (suite *YaqpgTestSuite) TestMustStartDefaultQueuePanics() {

	require := suite.Require()

	orig := os.Getenv("DATABASE_URL")
	defer os.Setenv("DATABASE_URL", orig)
	os.Setenv("DATABASE_URL", "")

	require.Panics(func() { yaqpg.MustStartDefaultQueue() })

}

func (suite *YaqpgTestSuite) TestMustStartDefaultQueueOK() {

	require := suite.Require()
	require.NotPanics(func() { yaqpg.MustStartDefaultQueue() })

}

func (suite *YaqpgTestSuite) TestMustCountPanics() {

	require := suite.Require()
	orig := suite.Queue.TableName
	defer func() { suite.Queue.TableName = orig }()
	suite.Queue.TableName = "syntax/error/here"

	require.Panics(func() { suite.Queue.MustCount() })

}

func (suite *YaqpgTestSuite) TestMustCountOK() {

	require := suite.Require()

	require.NotPanics(func() { suite.Queue.MustCount() })

}

func (suite *YaqpgTestSuite) TestMustCountReadyPanics() {

	require := suite.Require()
	orig := suite.Queue.TableName
	defer func() { suite.Queue.TableName = orig }()
	suite.Queue.TableName = "syntax/error/here"

	require.Panics(func() { suite.Queue.MustCountReady() })

}

func (suite *YaqpgTestSuite) TestMustCountReadyOK() {

	require := suite.Require()

	require.NotPanics(func() { suite.Queue.MustCountReady() })

}
func (suite *YaqpgTestSuite) TestMustCountPendingPanics() {

	require := suite.Require()
	orig := suite.Queue.TableName
	defer func() { suite.Queue.TableName = orig }()
	suite.Queue.TableName = "syntax/error/here"

	require.Panics(func() { suite.Queue.MustCountPending() })

}

func (suite *YaqpgTestSuite) TestMustCountPendingOK() {

	require := suite.Require()

	require.NotPanics(func() { suite.Queue.MustCountPending() })

}

func (suite *YaqpgTestSuite) TestLogCountsOK() {

	require := suite.Require()

	require.NoError(suite.Queue.Add("1", []byte("x")))
	require.NoError(suite.Queue.Add("2", []byte("x")))
	require.NoError(suite.Queue.Add("3", []byte("x")))
	require.NoError(suite.Queue.AddDelayed("4", []byte("x"), time.Minute))
	require.NoError(suite.Queue.AddDelayed("5", []byte("x"), time.Minute))
	suite.Logger.Clear()
	suite.Queue.LogCounts()
	suite.AssertLogged("item count: 5 (ready: 3, pending: 2)")
}

func (suite *YaqpgTestSuite) TestDefaultBackoffDelay() {

	require := suite.Require()

	// Zero gets zero
	orig := yaqpg.DefaultReprocessDelay
	yaqpg.DefaultReprocessDelay = 0
	defer func() { yaqpg.DefaultReprocessDelay = orig }()
	require.Equal(yaqpg.DefaultReprocessDelay,
		yaqpg.DefaultBackoffDelay(11111), "zero delay for many attempts")

	yaqpg.DefaultReprocessDelay = time.Second
	require.Equal(time.Second,
		yaqpg.DefaultBackoffDelay(1), "default for <2 attempts")

	morig := yaqpg.DefaultMaxReprocessDelay
	defer func() { yaqpg.DefaultReprocessDelay = morig }()
	yaqpg.DefaultMaxReprocessDelay = time.Second * 100

	// 5 attempts at 1 sec delay should get us 32 seconds
	require.Equal(time.Second*32,
		yaqpg.DefaultBackoffDelay(5), "")

	// 100 attempts is way over!
	require.Equal(yaqpg.DefaultMaxReprocessDelay,
		yaqpg.DefaultBackoffDelay(100), "")

}

func (suite *YaqpgTestSuite) TestDefaultStableDelay() {

	require := suite.Require()

	require.Equal(yaqpg.DefaultReprocessDelay,
		yaqpg.DefaultStableDelay(123))
}

func (suite *YaqpgTestSuite) TestAddError() {

	require := suite.Require()

	// This is getting to be a tedious way of creating errors, but...
	orig := suite.Queue.TableName
	defer func() { suite.Queue.TableName = orig }()
	suite.Queue.TableName = "syntax/error/here"

	err := suite.Queue.Add("someident", []byte("x"))
	require.ErrorContains(err, "SQLSTATE 42601")

	suite.AssertLogged(
		"adding someident",
		"adding someident: ERROR: syntax error at or near \"/\" (SQLSTATE 42601)",
	)

}

func (suite *YaqpgTestSuite) TestAddOK() {

	require := suite.Require()

	err := suite.Queue.Add("someident", []byte("x"))
	require.NoError(err)
	c, err := suite.Queue.CountReady()
	require.NoError(err)
	require.Equal(1, c, "ready count")
	suite.AssertLogged(
		"adding someident",
		"adding someident: success",
	)

}

func (suite *YaqpgTestSuite) TestAddDelayedError() {

	require := suite.Require()

	// This is getting to be a tedious way of creating errors, but...
	orig := suite.Queue.TableName
	defer func() { suite.Queue.TableName = orig }()
	suite.Queue.TableName = "syntax/error/here"

	err := suite.Queue.AddDelayed("someident", []byte("x"), time.Minute)
	require.ErrorContains(err, "SQLSTATE 42601")

	// note that all "now" is the zero time in our test queue
	suite.AssertLogged(
		"adding someident for 0001-01-01 00:01:00",
		"adding someident for 0001-01-01 00:01:00: ERROR: syntax error at or near \"/\" (SQLSTATE 42601)",
	)

}

func (suite *YaqpgTestSuite) TestAddDelayedOK() {

	require := suite.Require()

	err := suite.Queue.AddDelayed("someident", []byte("x"), time.Minute)
	require.NoError(err)
	r, err := suite.Queue.CountReady()
	require.NoError(err)
	require.Equal(0, r, "ready count")
	p, err := suite.Queue.CountPending()
	require.NoError(err)
	require.Equal(1, p, "pending count")
	suite.AssertLogged(
		"adding someident for 0001-01-01 00:01:00",
		"adding someident for 0001-01-01 00:01:00: success",
	)

}

func (suite *YaqpgTestSuite) TestFillSingleBatchOK() {

	require := suite.Require()

	yaqpg.FillBatchSize = 20

	err := suite.Queue.Fill(10, 1, time.Second)
	require.NoError(err)
	r, err := suite.Queue.CountReady()
	require.NoError(err)
	require.Equal(0, r, "ready count")
	p, err := suite.Queue.CountPending()
	require.NoError(err)
	require.Equal(10, p, "pending count")

}

func (suite *YaqpgTestSuite) TestFillMultiBatchOK() {

	require := suite.Require()

	yaqpg.FillBatchSize = 4

	err := suite.Queue.Fill(11, 1, time.Second)
	require.NoError(err)
	r, err := suite.Queue.CountReady()
	require.NoError(err)
	require.Equal(0, r, "ready count")
	p, err := suite.Queue.CountPending()
	require.NoError(err)
	require.Equal(11, p, "pending count")

	// TODO: prove delay is distributed, that would be nice...

}

func (suite *YaqpgTestSuite) TestFillMultiBatchModError() {

	require := suite.Require()

	orig := suite.Queue.TableName
	defer func() { suite.Queue.TableName = orig }()
	suite.Queue.TableName = "syntax/error/here"

	yaqpg.FillBatchSize = 3
	err := suite.Queue.Fill(4, 1, time.Second)
	require.ErrorContains(err, "SQLSTATE 42601")

}

func (suite *YaqpgTestSuite) TestFillMultiBatchError() {

	require := suite.Require()

	orig := suite.Queue.TableName
	defer func() { suite.Queue.TableName = orig }()
	suite.Queue.TableName = "syntax/error/here"

	yaqpg.FillBatchSize = 4
	err := suite.Queue.Fill(8, 1, time.Second)
	require.ErrorContains(err, "SQLSTATE 42601")

}
