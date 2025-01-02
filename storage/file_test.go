package storage

import (
	"os"
	"testing"
)

func TestSaveData(t *testing.T) {
	testFile := "test_save_data.txt"
	testData := []byte("Hello, Database!")
	
	// Clean up
	defer os.Remove(testFile)
	
	// Test saving data
	err := SaveData(testFile, testData)
	if err != nil {
		t.Fatalf("SaveData failed: %v", err)
	}
	
	// Verify file exists and contains correct data
	data, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read saved file: %v", err)
	}
	
	if string(data) != string(testData) {
		t.Errorf("Expected %q, got %q", testData, data)
	}
}

func TestSaveDataAtomicity(t *testing.T) {
	testFile := "test_atomicity.txt"
	testData := []byte("Atomic write test")
	
	// Clean up
	defer os.Remove(testFile)
	
	// Test that temporary files are cleaned up on success
	err := SaveData(testFile, testData)
	if err != nil {
		t.Fatalf("SaveData failed: %v", err)
	}
	
	// Check that no temporary files remain
	entries, _ := os.ReadDir(".")
	for _, entry := range entries {
		if entry.Name() != testFile && 
		   len(entry.Name()) > len(testFile) && 
		   entry.Name()[:len(testFile)+4] == testFile+".tmp" {
			t.Errorf("Temporary file not cleaned up: %s", entry.Name())
		}
	}
}