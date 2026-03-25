# Benchmarking report showing: Disk Space Savings (%), Upload speed (s), Query Access duration (s).
from functools import wraps
from models.logger import get_logger
import fsspec
import time

def get_function_duration(benchmark_instance: BenchmarkData, flag: str):
    """Decorator factory to wrap GCS upload functions and capture upload speed.

    Args:
        benchmark_instance: Instance of BenchmarkData to update with timing data

    Returns:
        function: Decorator function
    """
    def decorator(func):
        @wraps(func)
        def TimerWrapper(*args, **kwargs):
            """TimerWrapper Docstring"""
            start = time.time()
            result = func(*args, **kwargs)
            if flag is 'u':
                benchmark_instance.upload_speed = time.time() - start
            if flag is 'q':
                benchmark_instance.query_access_duration = time.time() - start
            return result
        return TimerWrapper
    return decorator

class BenchmarkData():
    
    def __init__(self):
        self.csv_size = 0
        self.parquet_size = 0
        self.upload_speed = 0
        self.query_access_duration = 0
        
    def _calculate_disk_space_savings(self) -> float:
        """Calculates disk space savings of .csv to .parquet conversion

        Returns:
            float: percent disk space savings as a float
        """
        return ((self.csv_size - self.parquet_size) / self.csv_size) * 100
    
    # setters - csv_size, parquet_size
    # sums file size across all .csv and .parquet files
    def add_to_csv_size(self, size: int):
        self.csv_size += size
        
    def add_to_parquet_size(self, gcs_uri: str):
        fs = fsspec.filesystem("gcs")
        info = fs.info(gcs_uri)

        self.parquet_size += info['size']
    
    def create_audit_log(self):
        logger = get_logger(__name__, 'audit.log')
        
        report = f"""
        
        ============================================
                    BENCHMARKING REPORT
        ============================================
        Total CSV Size: {(self.csv_size / (1000000)):.2f} MB
        Total Parquet Size: {(self.parquet_size / (1000000)):.2f} MB    
        ---    
        Disk Space Savings:     {self._calculate_disk_space_savings():.2f}%
        Upload Speed:           {self.upload_speed:.2f} s
        Query Access Duration:  {self.query_access_duration:.2f} s
        """
        
        logger.info(report)
