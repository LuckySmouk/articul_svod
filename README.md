# Prototype Data Processing Template

This repository provides a simple prototype template for processing Excel data files with Python. It includes three modular scripts demonstrating common data operations:

* **del-dubli\_1.py**: Removes duplicate rows from an Excel file.
* **sort\_artic\_2.py**: Extracts and normalizes article numbers ("артикулы") and adds variants as additional columns.
* **end\_4.py**: Matches article numbers to VTRAC codes, finds common prefixes, and fills missing values in parallel.

---

## Features

1. **Deduplication** (`del-dubli_1.py`)

   * Reads `in.xlsx` with pandas.
   * Drops duplicate rows, keeping the first occurrence.
   * Outputs cleaned data to `table_in.xlsx`.

2. **Article Number Extraction** (`sort_artic_2.py`)

   * Cleans and normalizes article codes (removes spaces, dashes, slashes).
   * Extracts variants from the main code and from nomenclature text.
   * Writes results into `tabl_out2.xlsx`, adding columns `Доп. Артикул 1...n`.

3. **VTRAC Matching** (`end_4.py`)

   * Loads consolidated Excel data (`in/all_out.xlsx`).
   * Builds a mapping of article codes to VTRAC codes using rows where VTRAC is present.
   * In parallel, fills missing VTRAC values based on code matches.
   * Finds common VTRAC prefixes and adds a `VTRAC` column.
   * Saves the final output to `out/output_4.xlsx`.

---

## Getting Started

### Prerequisites

* Python 3.8 or above
* `pandas`
* `openpyxl`
* `loguru`

Install dependencies with:

```bash
pip install pandas openpyxl loguru
```

### Usage

1. **Remove duplicates**:

   ```bash
   python del-dubli_1.py
   ```

   * Input: `in.xlsx`
   * Output: `table_in.xlsx`

2. **Extract article variants**:

   ```bash
   python sort_artic_2.py
   ```

   * Input: `tabl_in.xlsx`
   * Output: `tabl_out2.xlsx`

3. **Match VTRAC codes**:

   ```bash
   python end_4.py
   ```

   * Input: `in/all_out.xlsx`
   * Output: `out/output_4.xlsx`

---

## Project Structure

```
├── del-dubli_1.py       # Deduplication script
├── sort_artic_2.py      # Article parsing & variant extraction
├── end_4.py             # VTRAC matching & prefix aggregation
├── in/                   # Folder for input files
│   └── all_out.xlsx
├── out/                  # Folder for outputs
├── tabl_in.xlsx          # Intermediate file
└── tabl_out2.xlsx        # Results of article extraction
```

---

## Contributing

This template serves as a starting point for your own data processing needs. Feel free to adapt and extend the scripts to fit your workflow.

---

## License

This project is provided "as-is" without any warranty. Use and modify at your own discretion.
