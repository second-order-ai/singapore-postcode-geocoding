# singapore-postcode-geocoding

[![Powered by Kedro](https://img.shields.io/badge/powered_by-kedro-ffc900?logo=kedro)](https://kedro.org)

## Overview

A comprehensive system for automatically identifying, validating, and geocoding Singapore postcodes from tabular data. The system can intelligently detect postcode columns in DataFrames, extract postcodes from text fields using regex patterns, validate them against Singapore's postcode standards, and merge with geographic data.

Take a look at the [Kedro documentation](https://docs.kedro.org) to get started.

## App Information

The Streamlit web application is available at: [https://sg-postcode-geocoding.streamlit.app/](https://sg-postcode-geocoding.streamlit.app/)

The app code is located in `src/singapore_postcode_geocoding/app/`

## Usage Example: Geocoding with Python Dictionaries

Here's a complete example of how to use the Singapore postcode geocoding system with pure Python dictionaries:

### 1. Setup and Configuration

```python
import pandas as pd
from singapore_postcode_geocoding.pipelines.postcode_identification.auto_identification_classes_pipeline.nodes import (
    find_best_postcode_column,
    auto_convert_postcodes
)

# Validation configuration (based on conf/base/parameters.yml)
validation_config = {
    "range": {
        "int": [18906, 918146],  # Valid Singapore postcode range
        "len": [5, 6]            # Valid postcode length
    },
    "drop_incorrect": False,
    "keep_validation_fields": True,
    "keep_formatted_postcode_field": True,
    "validation_field_names": {
        "candidate_postcode": "CANDIDATE_POSTCODE",
        "extracted_postcode": "EXTRACTED_POSTCODE", 
        "correct_input_flag": "CORRECT_INPUT_POSTCODE",
        "incorrect_reason": "INCORRECT_INPUT_POSTCODE_REASON",
        "formatted_postcode": "FORMATTED_POSTCODE"
    }
}

# Auto-identification configuration
auto_identify_config = {
    "candidate_columns": None,  # Test all columns if None
    "regex_pattern": r"((?<!\d)\d{5,6}(?!\d))",  # Singapore postcode pattern
    "sample_size": 100,         # Sample size for testing
    "success_threshold": 0.1,   # Minimum success rate required
    "seed": 42                  # Random seed for reproducibility
}
```

### 2. Sample Data Setup

```python
# Sample DataFrame with potential postcode columns
sample_df = pd.DataFrame({
    "address": [
        "123 Orchard Road, Singapore 238801",
        "Marina Bay Sands, 10 Bayfront Avenue, Singapore 018956", 
        "No postcode here",
        "456 Bugis Street Singapore 188364"
    ],
    "postcode": ["238801", "018956", "invalid", "188364"],
    "location": ["238801 Orchard", "018956 Marina", "Random text", "188364 Bugis"],
    "postal_code": ["238801", "018956", "", "188364"],
    "other_data": ["data1", "data2", "data3", "data4"]
})

# Load the complete Singapore postcodes master list from GitHub
master_postcodes_url = "https://github.com/second-order-ai/singapore-postcode-geocoding/raw/refs/heads/main/data/03_primary/singapore_postcodes_masterlist.parquet/2025-02-11T21.27.51.862Z/singapore_postcodes_masterlist.parquet"
master_postcodes = pd.read_parquet(master_postcodes_url)

# The master postcodes DataFrame contains all valid Singapore postcodes with geocoding data
print(f"Loaded {len(master_postcodes)} valid Singapore postcodes")
print("Columns:", list(master_postcodes.columns))
```

### 3. Find Best Postcode Column

```python
# Find the best postcode column from your DataFrame
test_results = find_best_postcode_column(
    df=sample_df,
    validation_config=validation_config,
    master_postcodes=master_postcodes,
    auto_identify_config=auto_identify_config
)

print("Column test results:")
print(test_results)
# Output shows CONVERSION_SUCCESS_RATE, COLUMN, METHOD, and REGEX_PATTERN
```

### 4. Automatic Conversion

```python
# Automatically convert postcodes using the best method
converted_df, success, test_results = auto_convert_postcodes(
    df=sample_df,
    validation_config=validation_config,
    master_postcodes=master_postcodes,
    auto_identify_config=auto_identify_config
)

if success:
    print("✅ Postcode conversion successful!")
    print(f"Converted DataFrame shape: {converted_df.shape}")
    print("\nFormatted postcodes:")
    print(converted_df["FORMATTED_POSTCODE"].value_counts())
    
    # The converted DataFrame now includes:
    # - FORMATTED_POSTCODE: Standardized 6-digit postcodes
    # - CORRECT_INPUT_POSTCODE: Boolean flag for valid postcodes
    # - INCORRECT_INPUT_POSTCODE_REASON: Reason for invalid postcodes
    # - Plus all your original data columns
    
else:
    print("❌ Postcode conversion failed")
    print("Best success rate:", test_results.iloc[0]["CONVERSION_SUCCESS_RATE"] if not test_results.empty else 0)
```

### 5. Key Features

**Intelligent Column Detection**: The system automatically tests all columns (or specified ones) using:
- **Direct conversion**: Treats column values as postcodes directly
- **Indirect extraction**: Uses regex to extract postcodes from text fields

**Comprehensive Validation Pipeline**: Each postcode goes through:
1. Numeric validation 
2. Integer check
3. Range validation (18906-918146)
4. Length validation (5-6 digits)
5. Master dataset validation
6. Formatting to 6-digit strings with leading zeros

**Flexible Configuration**: All aspects are configurable via dictionaries:
- Validation rules and field name mappings
- Regex patterns for extraction
- Success thresholds for automatic selection
- Sample sizes for testing performance

**Real-world Data Handling**: Designed to handle:
- Postcodes embedded in address fields
- Inconsistent formatting (leading zeros, mixed case)
- Unexpectedly named columns
- Mixed valid/invalid data

### 6. Expected Output Format

Successful conversion produces a DataFrame with additional validation columns:
- `FORMATTED_POSTCODE`: Standardized 6-digit postcodes (e.g., "018906")
- `CORRECT_INPUT_POSTCODE`: Boolean flag indicating valid postcodes
- `INCORRECT_INPUT_POSTCODE_REASON`: Detailed reason for invalid postcodes ("NOT_NUMERIC", "OUT_OF_RANGE", "NOT_IN_MASTER_DATASET", etc.)
- Original data columns preserved

This system enables robust postcode processing for Singapore address data, making it ideal for geocoding applications, data cleaning, and geographic analysis.

## Installation

### Install from GitHub

You can install this package directly from GitHub to use in your own projects:

```bash
# Install the latest version from main branch
pip install git+https://github.com/second-order-ai/singapore-postcode-geocoding.git

# Or install from a specific branch
pip install git+https://github.com/second-order-ai/singapore-postcode-geocoding.git@branch-name

# Or install from a specific tag/release
pip install git+https://github.com/second-order-ai/singapore-postcode-geocoding.git@v1.0.0
```

> **Note**: If you encounter import errors with `singapore_postcode_validation` module, make sure you're using the latest version from the main branch which includes the proper package structure fixes.

### Using uv (recommended for this project)

```bash
uv add git+https://github.com/second-order-ai/singapore-postcode-geocoding.git
```

### Add to requirements.txt

In your project's `requirements.txt`:
```
singapore-postcode-geocoding @ git+https://github.com/second-order-ai/singapore-postcode-geocoding.git
```

### Usage in External Projects

After installation, you can use the geocoding system in your own projects:

```python
import pandas as pd
from singapore_postcode_geocoding.pipelines.postcode_identification.auto_identification_classes_pipeline.nodes import (
    find_best_postcode_column,
    auto_convert_postcodes
)

# Load your data
df = pd.read_csv("your_data.csv")

# Load master postcodes
master_postcodes_url = "https://github.com/second-order-ai/singapore-postcode-geocoding/raw/refs/heads/main/data/03_primary/singapore_postcodes_masterlist.parquet/2025-02-11T21.27.51.862Z/singapore_postcodes_masterlist.parquet"
master_postcodes = pd.read_parquet(master_postcodes_url)

# Configure validation and auto-identification
validation_config = {
    "range": {"int": [18906, 918146], "len": [5, 6]},
    "drop_incorrect": False,
    "keep_validation_fields": True,
    "keep_formatted_postcode_field": True,
    "validation_field_names": {
        "candidate_postcode": "CANDIDATE_POSTCODE",
        "extracted_postcode": "EXTRACTED_POSTCODE", 
        "correct_input_flag": "CORRECT_INPUT_POSTCODE",
        "incorrect_reason": "INCORRECT_INPUT_POSTCODE_REASON",
        "formatted_postcode": "FORMATTED_POSTCODE"
    }
}

auto_identify_config = {
    "candidate_columns": None,
    "regex_pattern": r"((?<!\d)\d{5,6}(?!\d))",
    "sample_size": 100,
    "success_threshold": 0.1,
    "seed": 42
}

# Automatically identify and convert postcodes
converted_df, success, test_results = auto_convert_postcodes(
    df=df,
    validation_config=validation_config,
    master_postcodes=master_postcodes,
    auto_identify_config=auto_identify_config
)

if success:
    print("✅ Postcodes successfully identified and converted!")
    # Your geocoded data is now in converted_df
else:
    print("❌ No suitable postcode column found")
```

## Rules and guidelines

In order to get the best out of the template:

* Don't remove any lines from the `.gitignore` file we provide
* Make sure your results can be reproduced by following a [data engineering convention](https://docs.kedro.org/en/stable/faq/faq.html#what-is-data-engineering-convention)
* Don't commit data to your repository
* Don't commit any credentials or your local configuration to your repository. Keep all your credentials and local configuration in `conf/local/`

## How to install dependencies

Use `uv`.

## How to run your Kedro pipeline

You can run your Kedro project with:

```
kedro run
```

## How to test your Kedro project

Have a look at the files `src/tests/test_run.py` and `src/tests/pipelines/data_science/test_pipeline.py` for instructions on how to write your tests. Run the tests as follows:

```
pytest
```

To configure the coverage threshold, look at the `.coveragerc` file.

## Project dependencies

To see and update the dependency requirements for your project use `requirements.txt`. Install the project requirements with `pip install -r requirements.txt`.

[Further information about project dependencies](https://docs.kedro.org/en/stable/kedro_project_setup/dependencies.html#project-specific-dependencies)

## How to work with Kedro and notebooks

> Note: Using `kedro jupyter` or `kedro ipython` to run your notebook provides these variables in scope: `catalog`, `context`, `pipelines` and `session`.
>
> Jupyter, JupyterLab, and IPython are already included in the project requirements by default, so once you have run `pip install -r requirements.txt` you will not need to take any extra steps before you use them.

### Jupyter
To use Jupyter notebooks in your Kedro project, you need to install Jupyter:

```
pip install jupyter
```

After installing Jupyter, you can start a local notebook server:

```
kedro jupyter notebook
```

### JupyterLab
To use JupyterLab, you need to install it:

```
pip install jupyterlab
```

You can also start JupyterLab:

```
kedro jupyter lab
```

### IPython
And if you want to run an IPython session:

```
kedro ipython
```

### How to ignore notebook output cells in `git`
To automatically strip out all output cell contents before committing to `git`, you can use tools like [`nbstripout`](https://github.com/kynan/nbstripout). For example, you can add a hook in `.git/config` with `nbstripout --install`. This will run `nbstripout` before anything is committed to `git`.

> *Note:* Your output cells will be retained locally.

[Further information about using notebooks for experiments within Kedro projects](https://docs.kedro.org/en/develop/notebooks_and_ipython/kedro_and_notebooks.html).
## Package your Kedro project

[Further information about building project documentation and packaging your project](https://docs.kedro.org/en/stable/tutorial/package_a_project.html).
