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
