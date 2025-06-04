APP_CONFIG = {
    "page_config": {
        "layout": "wide",
        "page_title": "Singapore postcode geocoder"
    },
    "validation_config": {
        "validation_field_names": {
            "correct_input_flag": "CORRECT_INPUT_POSTCODE",
            "incorrect_reason": "INCORRECT_INPUT_POSTCODE_REASON",
            "formatted_postcode": "FORMATTED_POSTCODE",
            "candidate_postcode": "POSTCODE",
            "extracted_postcode": "EXTRACTED"
        },
        "range": {"int": [18906, 918146], "len": [5, 6]},
        "drop_incorrect": False,
        "keep_formatted_postcode_field": True,
        "keep_validation_fields": True
    },
    "auto_identify_config": {
        "regex_pattern": r"(?<!\d)(\d{5,6})(?!\d)",
        "success_threshold": 0.1
    }
} 