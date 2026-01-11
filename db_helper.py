import os

def get_db_disk_size(db_path):
    # Returns size in Mebibytes
    size_bytes = int(os.path.getsize(db_path))
    return float(size_bytes / (1024 * 1024))