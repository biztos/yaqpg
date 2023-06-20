// yaqpg_test.go

package yaqpg_test

import (
	"os"

	"github.com/biztos/yaqpg"
)

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
