"""Constants and configuration for the NX1 SDK."""

# File format options with their default values and allowed values
FILE_FORMAT_OPTIONS = {
    "csv": {
        "header": {"default": "true", "allowed": ["true", "false"]},
        "inferSchema": {"default": "true", "allowed": ["true", "false"]},
        "delimiter": {"default": ","},
        "quote": {"default": '"'},
        "dateFormat": {"default": "yyyy-MM-dd"},
        "timestampFormat": {"default": "yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]"}
    },
    "parquet": {},
    "orc": {},
    "xml": {},
    "xls": {
        "header": {"default": "true", "allowed": ["true", "false"]},
        "sheet_name": {"default": "0"}
    },
    "xlsx": {
        "header": {"default": "true", "allowed": ["true", "false"]},
        "sheet_name": {"default": "0"}
    }
}

# Map file extensions to format names
FILE_EXTENSION_MAP = {
    'csv': 'csv',
    'parquet': 'parquet',
    'orc': 'orc',
    'xml': 'xml',
    'xls': 'xls',
    'xlsx': 'xls'  # xlsx uses same format as xls
}

# Content types for file uploads
CONTENT_TYPES = {
    'csv': 'text/csv',
    'parquet': 'application/parquet',
    'orc': 'application/orc',
    'xml': 'application/xml',
    'xls': 'application/vnd.ms-excel',
    'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
}
