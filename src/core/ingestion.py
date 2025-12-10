import os
import csv
import polars as pl
import charset_normalizer
from pathlib import Path
from typing import Tuple, Dict

class IngestionEngine:
    """
    Handles file validation, encoding detection, and the "Stream Split" 
    strategy to separate good data from bad data.
    """

    def validate_file_access(self, file_path: str) -> Dict[str, bool]:
        """
        Checks if file exists, is readable, and not locked.
        """
        path = Path(file_path)
        if not path.exists():
            return {"valid": False, "error": "File does not exist."}
        
        # "Touch Test": Try to acquire a read handle
        try:
            with open(path, 'rb') as f:
                # Read signature bytes while we are here
                sig = f.read(4) 
        except PermissionError:
            return {"valid": False, "error": "Permission Denied. File might be open in another program."}
        except Exception as e:
            return {"valid": False, "error": f"System Error: {str(e)}"}
            
        return {"valid": True, "error": None}

    def detect_encoding(self, file_path: str) -> Dict[str, any]:
        """
        Uses charset-normalizer on the first 50KB.
        Returns encoding and confidence.
        """
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(50_000) # Read first 50KB
                result = charset_normalizer.detect(raw_data)
                
            return {
                "encoding": result['encoding'] or 'utf-8',
                "confidence": result['confidence'] or 0.0
            }
        except Exception as e:
            return {"encoding": "utf-8", "confidence": 0.0, "error": str(e)}

    def stream_clean_and_split(self, input_path: str, clean_out_path: str, quarantine_out_path: str, encoding: str = 'utf-8') -> Dict[str, int]:
        """
        The "Stream Cleaner":
        1. Reads raw file line-by-line.
        2. Infers expected column count from header.
        3. Writes valid rows to `clean_out_path`.
        4. Writes ragged rows to `quarantine_out_path`.
        """
        total_rows = 0
        good_rows = 0
        bad_rows = 0
        expected_cols = 0
        
        # Python's CSV module is robust for streaming
        with open(input_path, 'r', encoding=encoding, errors='replace') as infile, \
             open(clean_out_path, 'w', encoding='utf-8', newline='') as clean_file, \
             open(quarantine_out_path, 'w', encoding='utf-8', newline='') as bad_file:
            
            # Using csv.reader to handle quoting/escaping logic automatically
            reader = csv.reader(infile)
            clean_writer = csv.writer(clean_file)
            bad_writer = csv.writer(bad_file)
            
            try:
                # Process Header
                header = next(reader)
                expected_cols = len(header)
                clean_writer.writerow(header) # Write header to clean file
                # Write header to quarantine too (so we can load it later)
                bad_writer.writerow(header) 
                
            except StopIteration:
                return {"status": "empty_file"}

            # Process Data Stream
            for row in reader:
                total_rows += 1
                if len(row) == expected_cols:
                    clean_writer.writerow(row)
                    good_rows += 1
                else:
                    # Write the raw row to quarantine
                    bad_writer.writerow(row)
                    bad_rows += 1
                    
        return {
            "total": total_rows,
            "good": good_rows,
            "bad": bad_rows,
            "expected_cols": expected_cols
        }

    def convert_to_parquet(self, csv_path: str, parquet_path: str):
        """
        Converts the CLEAN csv to Parquet using Polars for high-speed downstream tasks.
        """
        try:
            # We can safely use polars now because the CSV is structurally clean
            df = pl.scan_csv(csv_path)
            df.sink_parquet(parquet_path)
            return True
        except Exception as e:
            print(f"Parquet Conversion Failed: {e}")
            return False
        
    # ... inside IngestionEngine class ...

    def merge_and_finalize(self, clean_csv_path: str, fixed_csv_path: str, final_parquet_path: str) -> Dict[str, int]:
        """
        Merges the original clean data with the user-repaired data 
        and converts everything to the Master Parquet file.
        """
        try:
            # 1. Read the Clean Data
            df_clean = pl.scan_csv(clean_csv_path)
            
            # 2. Read the Fixed Data (if exists)
            if os.path.exists(fixed_csv_path) and os.path.getsize(fixed_csv_path) > 0:
                # We need to ensure schemas match exactly
                # We read schema from clean data to enforce it on fixed data
                schema = pl.read_csv(clean_csv_path, n_rows=0).schema
                
                try:
                    df_fixed = pl.scan_csv(fixed_csv_path, schema=schema)
                    # Concatenate (Lazy)
                    df_final = pl.concat([df_clean, df_fixed])
                except Exception as e:
                    print(f"Merge Error: {e}. Ignoring fixed rows.")
                    df_final = df_clean
            else:
                df_final = df_clean

            # 3. Sink to Parquet
            df_final.sink_parquet(final_parquet_path)
            
            # 4. Count rows for confirmation
            final_count = pl.read_parquet(final_parquet_path).height
            return {"success": True, "total_rows": final_count}
            
        except Exception as e:
            return {"success": False, "error": str(e)}