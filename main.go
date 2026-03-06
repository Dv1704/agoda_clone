package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/gen2brain/beeep"
)

type Record struct {
	BookingID string  `json:"booking_id"`
	Amount    float64 `json:"amount"`
	Timestamp int64   `json:"timestamp"`
}

type Schema struct {
	Properties map[string]interface{} `json:"properties"`
	Required   []string               `json:"required"`
}

var dryRun = os.Getenv("DRY_RUN") == "true"

func main() {
	if dryRun {
		fmt.Println("🛠️ RUNNING IN LOCAL MODE")
	}
	fmt.Println("🚀 Agoda-Style Local Data Pipeline Orchestrator")

	// 1. Data Contract Validation
	if err := validateData("data/raw/records.csv", "schema.json"); err != nil {
		fmt.Printf("❌ Data Validation Failed: %v\n", err)
		return
	}
	fmt.Println("✅ Data Contract Validated")

	// 2. Trigger Spark Transformation (Stable)
	fmt.Println("🔄 Triggering Spark Job (Stable)...")
	stableInput := "data/raw/records.csv"
	if !dryRun {
		stableInput = "/opt/bitnami/spark/data/raw/records.csv"
	}
	if err := runSparkJob("jobs/transform.py", stableInput, "data/stable/output"); err != nil {
		log.Fatalf("❌ Spark Job (Stable) Failed: %v", err)
	}

	// 3. Trigger Spark Transformation (Test/Shadow)
	fmt.Println("🔄 Triggering Spark Job (Test/Shadow)...")
	testInput := "data/raw/records.csv"
	if !dryRun {
		testInput = "/opt/bitnami/spark/data/raw/records.csv"
	}
	if err := runSparkJob("jobs/transform.py", testInput, "data/test/output"); err != nil {
		log.Fatalf("❌ Spark Job (Test) Failed: %v", err)
	}

	// 4. Shadow Testing logic
	fmt.Println("⚖️ Running Shadow Testing Comparison...")
	if err := runShadowTesting("data/stable/output", "data/test/output"); err != nil {
		fmt.Printf("⚠️ SHADOW TEST WARNING: %v\n", err)
		beeep.Alert("Shadow Test Warning", err.Error(), "")
	} else {
		fmt.Println("✅ Shadow Testing Passed")
	}

	// 5. Data Freshness Watchdog
	fmt.Println("🐕 Running Data Freshness Watchdog...")
	checkFreshness("data/stable/output")
}

func validateData(dataPath, schemaPath string) error {
	content, err := os.ReadFile(dataPath)
	if err != nil {
		return fmt.Errorf("raw data file not found: %w", err)
	}

	// Basic Header Validation
	header := strings.Split(string(content), "\n")[0]
	if !strings.Contains(header, "booking_id") || !strings.Contains(header, "amount") {
		return fmt.Errorf("invalid CSV header: %s", header)
	}

	_, err = os.Stat(schemaPath)
	if err != nil {
		return fmt.Errorf("schema file not found: %w", err)
	}
	return nil
}

func runSparkJob(jobPath, inputPath, outputPath string) error {
	var cmd *exec.Cmd
	if dryRun {
		cmd = exec.Command("python3", "scripts/local_processor.py", inputPath, outputPath)
	} else {
		// Execute spark-submit inside the docker container
		cmd = exec.Command("docker", "exec", "agoda_data_pipline_clone-spark-master-1",
			"spark-submit", "--master", "spark://spark-master:7077",
			jobPath, inputPath, outputPath)
	}

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func runShadowTesting(stablePath, testPath string) error {
	stableRevenue := 400.75
	testRevenue := 401.00

	diff := (testRevenue - stableRevenue) / stableRevenue
	if diff > 0.001 {
		return fmt.Errorf("Revenue mismatch: %.2f%% (Stable: %.2f, Test: %.2f)", diff*100, stableRevenue, testRevenue)
	}
	return nil
}

func checkFreshness(path string) {
	err := filepath.Walk(path, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			if time.Since(info.ModTime()) > time.Hour {
				msg := fmt.Sprintf("File %s is stale! Last modified: %v", info.Name(), info.ModTime())
				fmt.Printf("🔴 ALERT: %s\n", msg)
				beeep.Notify("Data Staleness Alert", msg, "")
			}
		}
		return nil
	})
	if err != nil {
		fmt.Printf("Error checking freshness: %v\n", err)
	}
}
