package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"
)

func main() {
	f, _ := os.Create("data/raw/records.csv")
	defer f.Close()

	f.WriteString("booking_id,amount,timestamp\n")
	for i := 1; i <= 10; i++ {
		amount := 100.0 + rand.Float64()*500.0
		f.WriteString(fmt.Sprintf("BK%03d,%.2f,%d\n", i, amount, time.Now().Unix()))
	}
	fmt.Println("✅ Generated 10 fresh financial records in data/raw/records.csv")
}
