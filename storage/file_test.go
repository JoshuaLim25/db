package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSaveData(t *testing.T) {
	testFile := "test_save_data.txt"
	testData := []byte("Hello, Database!")
	
	// Clean up
	defer os.Remove(testFile)
	
	// Test saving data
	err := SaveData(testFile, testData)
	require.NoError(t, err, "SaveData should succeed")
	
	// Verify file exists and contains correct data
	data, err := os.ReadFile(testFile)
	require.NoError(t, err, "Should be able to read saved file")
	
	assert.Equal(t, testData, data, "File should contain the correct data")
}

func TestSaveDataAtomicity(t *testing.T) {
	testFile := "test_atomicity.txt"
	testData := []byte("Atomic write test")
	
	// Clean up
	defer os.Remove(testFile)
	
	// Test that temporary files are cleaned up on success
	err := SaveData(testFile, testData)
	require.NoError(t, err, "SaveData should succeed")
	
	// Check that no temporary files remain
	entries, err := os.ReadDir(".")
	require.NoError(t, err, "Should be able to read directory")
	
	for _, entry := range entries {
		if entry.Name() != testFile && 
		   len(entry.Name()) > len(testFile) && 
		   entry.Name()[:len(testFile)+4] == testFile+".tmp" {
			assert.Fail(t, "Temporary file not cleaned up", "Found temp file: %s", entry.Name())
		}
	}
}