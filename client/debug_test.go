package client

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDirectoryCreation(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "pglitedb-client-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dbPath := tmpDir + "/test-db"
	
	// Test our directory creation logic
	err = os.MkdirAll(dbPath, 0755)
	require.NoError(t, err)
	
	// Verify the directory exists
	_, err = os.Stat(dbPath)
	assert.NoError(t, err)
	
	// Verify it's a directory
	info, err := os.Stat(dbPath)
	require.NoError(t, err)
	assert.True(t, info.IsDir())
}