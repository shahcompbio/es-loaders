
## Filenames
CELLS_FILENAME = 'cells.tsv'
GENES_FILENAME = 'genes.tsv'
MATRIX_FILENAME = 'matrix.mtx'
JUNO_SAMPLES_FILENAME = 'samples.txt'
JUNO_MARKERS_FILENAME = 'markers.tsv'
SAMPLES_FILENAME = 'sample_metadata.json'
MARKER_GENES_FILENAME = 'marker_genes.json'

MARKER_GENES_URL = "https://raw.githubusercontent.com/shahcompbio/shahlab_apps/master/shahlab_apps/apps/cellassign/hgsc_v5_major.csv"

## Elasticsearch Index names
DASHBOARD_ENTRY_INDEX = "dashboard_entry"
DASHBOARD_DATA_PREFIX = "dashboard_cells_"
MARKER_GENES_INDEX = "marker_genes"
GENES_INDEX = "genes"

## Elasticsearch mappings
MARKER_GENES_MAPPING = {
    "settings": {
        "index": {
            "max_result_window": 50000
        }
    },
    'mappings': {
        "dynamic_templates": [
            {
                "string_values": {
                    "match": "*",
                    "match_mapping_type": "string",
                    "mapping": {
                        "type": "keyword"
                    }
                }
            }
        ]
    }
}

GENES_MAPPING = {
    "settings": {
        "index": {
            "max_result_window": 50000
        }
    },
    'mappings': {
        "dynamic_templates": [
            {
                "string_values": {
                    "match": "*",
                    "match_mapping_type": "string",
                    "mapping": {
                        "type": "keyword"
                    }
                }
            }
        ]
    }
}


CELLS_INDEX_MAPPING = {
    "settings": {
        "index": {
            "max_result_window": 50000,
            "mapping": {
                "nested_objects": {
                    "limit": 25000
                }
            }
        }
    },    
    'mappings': {
        "dynamic_templates": [
            {
                "string_values": {
                    "match": "*",
                    "match_mapping_type": "string",
                    "mapping": {
                        "type": "keyword"
                    }
                }
            }
        ],
        "properties": {
            "genes": {
                "type": "nested"
            }
        }
    }
}


DASHBOARD_ENTRY_INDEX_MAPPING = {
    "settings": {
        "index": {
            "max_result_window": 50000,
            "mapping": {
                "nested_objects": {
                    "limit": 25000
                }
            }
        }
    },    
    'mappings': {
        "dynamic_templates": [
            {
                "string_values": {
                    "match": "*",
                    "match_mapping_type": "string",
                    "mapping": {
                        "type": "keyword"
                    }
                }
            }
        ],
        "properties": {
            "samples": {
                "type": "nested"
            },
            "date": {
                "type": "date"
            }
        }
    }
}