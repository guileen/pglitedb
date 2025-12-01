package kv

import (
	"context"
	"os"
	"testing"

	"github.com/guileen/pglitedb/storage/shared"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteSkew(t *testing.T) {
	// Skip this test if running in a CI environment or when specifically requested to skip long tests
	if os.Getenv("SKIP_LONG_TESTS") == "true" {
		t.Skip("Skipping long write skew test")
	}

	kv, err := NewPebbleKV(DefaultPebbleConfig(t.TempDir()))
	require.NoError(t, err)
	defer kv.Close()

	ctx := context.Background()
	require.NoError(t, kv.Set(ctx, []byte("x"), []byte("0")))
	require.NoError(t, kv.Set(ctx, []byte("y"), []byte("0")))

	tx1, err := kv.NewTransaction(ctx)
	require.NoError(t, err)
	require.NoError(t, tx1.SetIsolation(shared.Serializable))
	
	tx2, err := kv.NewTransaction(ctx)
	require.NoError(t, err)
	require.NoError(t, tx2.SetIsolation(shared.Serializable))

	y1, err := tx1.Get([]byte("y"))
	require.NoError(t, err)
	if string(y1) == "0" {
		require.NoError(t, tx1.Set([]byte("x"), []byte("1")))
	}

	x2, err := tx2.Get([]byte("x"))
	require.NoError(t, err)
	if string(x2) == "0" {
		require.NoError(t, tx2.Set([]byte("y"), []byte("1")))
	}

	err1 := tx1.Commit()
	assert.NoError(t, err1)

	err2 := tx2.Commit()
	assert.Error(t, err2)
	assert.Equal(t, shared.ErrConflict, err2)
}

func TestSerializableIsolation(t *testing.T) {
	// Skip this test if running in a CI environment or when specifically requested to skip long tests
	if os.Getenv("SKIP_LONG_TESTS") == "true" {
		t.Skip("Skipping long serializable isolation test")
	}

	kv, err := NewPebbleKV(DefaultPebbleConfig(t.TempDir()))
	require.NoError(t, err)
	defer kv.Close()

	ctx := context.Background()
	require.NoError(t, kv.Set(ctx, []byte("counter"), []byte("100")))

	tx1, err := kv.NewTransaction(ctx)
	require.NoError(t, err)
	require.NoError(t, tx1.SetIsolation(shared.Serializable))
	
	tx2, err := kv.NewTransaction(ctx)
	require.NoError(t, err)
	require.NoError(t, tx2.SetIsolation(shared.Serializable))

	v1, err := tx1.Get([]byte("counter"))
	require.NoError(t, err)
	assert.Equal(t, "100", string(v1))

	v2, err := tx2.Get([]byte("counter"))
	require.NoError(t, err)
	assert.Equal(t, "100", string(v2))

	require.NoError(t, tx1.Set([]byte("counter"), []byte("150")))
	require.NoError(t, tx1.Commit())

	require.NoError(t, tx2.Set([]byte("counter"), []byte("200")))
	err = tx2.Commit()
	assert.Error(t, err)
	assert.Equal(t, shared.ErrConflict, err)
}

func TestReadCommittedNoConflict(t *testing.T) {
	// Skip this test if running in a CI environment or when specifically requested to skip long tests
	if os.Getenv("SKIP_LONG_TESTS") == "true" {
		t.Skip("Skipping long read committed no conflict test")
	}

	kv, err := NewPebbleKV(DefaultPebbleConfig(t.TempDir()))
	require.NoError(t, err)
	defer kv.Close()

	ctx := context.Background()
	require.NoError(t, kv.Set(ctx, []byte("x"), []byte("0")))

	tx1, err := kv.NewTransaction(ctx)
	require.NoError(t, err)
	
	tx2, err := kv.NewTransaction(ctx)
	require.NoError(t, err)

	v1, err := tx1.Get([]byte("x"))
	require.NoError(t, err)
	assert.Equal(t, "0", string(v1))

	require.NoError(t, tx2.Set([]byte("x"), []byte("1")))
	require.NoError(t, tx2.Commit())

	require.NoError(t, tx1.Set([]byte("x"), []byte("2")))
	err = tx1.Commit()
	assert.NoError(t, err)

	v, err := kv.Get(ctx, []byte("x"))
	require.NoError(t, err)
	assert.Equal(t, "2", string(v))
}