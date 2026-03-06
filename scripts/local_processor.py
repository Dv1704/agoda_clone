import sys
import os
import shutil
import time

def main(input_path, output_path):
    print(f"⚙️ [LOCAL PROCESSOR] Processing {input_path}...")
    time.sleep(1) 
    
    if os.path.exists(output_path):
        shutil.rmtree(output_path)
    os.makedirs(output_path)
    
    with open(os.path.join(output_path, "_SUCCESS"), "w") as f:
        f.write("")
    with open(os.path.join(output_path, "part-0000.parquet"), "w") as f:
        f.write("PROCESSED_DATA")
        
    print(f"✅ [LOCAL PROCESSOR] Saved results to {output_path}")

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
