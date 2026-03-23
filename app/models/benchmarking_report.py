# Benchmarking report showing: Disk Space Savings (%), Upload speed (s), Query Access duration (s).
from functools import wraps
from models.logger import get_logger
import pyarrow.fs as fs
import time

logger = get_logger(__name__, 'audit.log')

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
    
    def get_upload_speed(self, func: function) -> function:
        """Higher-order function to wrap GCS upload at call time and capture the speed.

        Args:
            func (function): GCS upload function

        Returns:
            function: timer wrapper function
        """
        @wraps(func)
        def TimerWrapper(*args, **kwargs):
            """TimerWrapper Docstring"""
            logger.debug('Timer started')
            start = time.time()
            func(*args, **kwargs)
            self.upload_speed = time.time() - start            
        return TimerWrapper
    
    # setters - csv_size, parquet_size
    def set_csv_size(self, size: int):
        self.csv_size = size
        
    def set_parquet_size(self, gcs_uri):
        gcs = fs.GcsFileSystem() # uses pyarrow
        info = gcs.get_file_info(gcs_uri)

        self.parquet_size = info.size
    
    def print_report(self):
        report = f"""
        ============================================
                    BENCHMARKING REPORT
        ============================================
        Disk Space Savings:     {self._calculate_disk_space_savings():.2f}%
        Upload Speed:           {self.upload_speed:.2f}s
        Query Access Duration:  TBD (s)
        """.strip()
        
        logger.info(report)
