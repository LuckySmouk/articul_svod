import os
import sys
import pandas as pd
from loguru import logger
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from typing import List, Dict, Any, Optional, Tuple, Set

# Configureable Constants


class Config:
    # Input and Output File Paths
    INPUT_PATH_TABL_OUT = "tabl_out.xlsx"
    INPUT_PATH_CATALOG = "catalog.xlsx"
    OUTPUT_PATH = "all_out_antro2.xlsx"

    # Logging Configuration
    LOG_FILE = "logfile.log"
    LOG_ROTATION = "10 MB"
    LOG_LEVEL = "DEBUG"  # Changed to DEBUG for detailed logging
    CONSOLE_LOG_LEVEL = "INFO"  # Separate console logging level

    # Processing Configuration
    MAX_WORKERS = 10
    MAX_VTRAC_COLUMNS = 5  # Maximum number of VTRAC columns to create

    # Column Configurations
    ADDITIONAL_ARTICLE_COLS = [
        "Артикул",
        "Доп. Артикул 1",
        "Доп. Артикул 2",
        "Доп. Артикул 3",
        "Доп. Артикул 4",
        "Доп. Артикул 5"
    ]
    CATALOG_COLS = ["Артикул", "Артикул аналога", "VTRAC"]


# Configure logging with both file and console outputs
logger.remove()  # Remove default handler
logger.add(Config.LOG_FILE, rotation=Config.LOG_ROTATION,
           level=Config.LOG_LEVEL)
logger.add(sys.stdout, level=Config.CONSOLE_LOG_LEVEL,
           colorize=True)  # Add console output with colors


class VTRACFinder:
    def __init__(self, tabl_out_path: str, catalog_path: str):
        """
        Initialize the VTRAC finder with input files
        """
        self.tabl_out_path = tabl_out_path
        self.catalog_path = catalog_path

        # Dictionaries to store catalog mappings
        self.article_dict: Dict[str, str] = {}
        self.analog_dict: Dict[str, str] = {}

        # Load and preprocess data
        self.load_catalog()

    def load_catalog(self):
        """
        Load catalog and create lookup dictionaries
        """
        logger.info("Loading catalog data...")

        try:
            # Read catalog with explicit string type casting
            catalog_df = pd.read_excel(
                self.catalog_path,
                usecols=Config.CATALOG_COLS,
                dtype={col: str for col in Config.CATALOG_COLS},
                engine='openpyxl'
            )

            # Log catalog size
            logger.info(f"Loaded catalog with {len(catalog_df)} entries")

            # Create dictionaries for quick lookups
            for _, row in catalog_df.iterrows():
                # Process main article
                key_article = row['Артикул']
                key_article = str(key_article).strip(
                ) if not pd.isna(key_article) else ""

                vtrac = row['VTRAC']
                vtrac = str(vtrac).strip() if not pd.isna(vtrac) else ""

                if key_article and key_article not in ('', 'nan') and vtrac and vtrac not in ('', 'nan'):
                    self.article_dict[key_article] = vtrac

                # Process analog article
                key_analog = row['Артикул аналога']
                key_analog = str(key_analog).strip(
                ) if not pd.isna(key_analog) else ""

                if key_analog and key_analog not in ('', 'nan') and vtrac and vtrac not in ('', 'nan'):
                    self.analog_dict[key_analog] = vtrac

            # Log dictionary sizes
            logger.info(
                f"Catalog dictionaries created: {len(self.article_dict)} articles, "
                f"{len(self.analog_dict)} analogs"
            )

            # Log some examples for debugging
            article_samples = list(self.article_dict.items())[:5]
            analog_samples = list(self.analog_dict.items())[:5]
            logger.debug(f"Article dictionary samples: {article_samples}")
            logger.debug(f"Analog dictionary samples: {analog_samples}")

        except Exception as e:
            logger.error(f"Error loading catalog: {e}")
            raise

    def find_vtrac(self, article: str) -> List[str]:
        """
        Find VTRAC for an article with flexible matching
        
        Returns:
        - List of unique VTRACs found
        """
        found_vtracs = []

        if not article or pd.isna(article) or article.strip() == '':
            return found_vtracs

        # Normalize article
        article = article.strip()

        # Log search for debugging
        logger.debug(f"Searching for VTRAC matching article: '{article}'")

        # Step 1: Exact match first
        exact_vtrac_article = self.article_dict.get(article)
        if exact_vtrac_article and pd.notna(exact_vtrac_article) and exact_vtrac_article.strip() != '':
            logger.debug(
                f"Found exact match in article_dict: {exact_vtrac_article}")
            found_vtracs.append(exact_vtrac_article)

        exact_vtrac_analog = self.analog_dict.get(article)
        if exact_vtrac_analog and pd.notna(exact_vtrac_analog) and exact_vtrac_analog.strip() != '' and exact_vtrac_analog not in found_vtracs:
            logger.debug(
                f"Found exact match in analog_dict: {exact_vtrac_analog}")
            found_vtracs.append(exact_vtrac_analog)

        # Step 2: Flexible prefix matching
        # Check if catalog article starts with our article
        for catalog_key, vtrac in self.article_dict.items():
            if not catalog_key or pd.isna(catalog_key) or not vtrac or pd.isna(vtrac):
                continue

            catalog_key = catalog_key.strip()
            vtrac = vtrac.strip()

            # Check if article is a prefix of catalog key
            if catalog_key.startswith(article) and vtrac not in found_vtracs and vtrac != '':
                logger.debug(
                    f"Found prefix match in article_dict: {catalog_key} -> {vtrac}")
                found_vtracs.append(vtrac)

        # Also check analog articles
        for catalog_key, vtrac in self.analog_dict.items():
            if not catalog_key or pd.isna(catalog_key) or not vtrac or pd.isna(vtrac):
                continue

            catalog_key = catalog_key.strip()
            vtrac = vtrac.strip()

            # Check if article is a prefix of catalog key
            if catalog_key.startswith(article) and vtrac not in found_vtracs and vtrac != '':
                logger.debug(
                    f"Found prefix match in analog_dict: {catalog_key} -> {vtrac}")
                found_vtracs.append(vtrac)

        if found_vtracs:
            logger.debug(
                f"Found {len(found_vtracs)} VTRACs for '{article}': {found_vtracs}")
        else:
            logger.debug(f"No VTRACs found for '{article}'")

        return found_vtracs

    def process_row(self, row: pd.Series) -> List[Optional[str]]:
        """
        Process a single row to find VTRACs
        
        Args:
            row: A pandas Series representing a row from tabl_out
        
        Returns:
            List of found VTRACs (up to MAX_VTRAC_COLUMNS)
        """
        found_vtracs = []

        # Keep track of which article led to which VTRAC for debugging
        article_vtrac_map = {}

        # Iterate through ALL article columns
        for col in Config.ADDITIONAL_ARTICLE_COLS:
            article = row[col]

            # Skip invalid articles
            if pd.isna(article) or not isinstance(article, str) or article.strip() == '':
                continue

            # Normalize article
            article_key = article.strip()

            # Find VTRACs for this article
            col_vtracs = self.find_vtrac(article_key)

            # Track which article produced which VTRACs
            if col_vtracs:
                article_vtrac_map[article_key] = col_vtracs

            # Add unique VTRACs
            for vtrac in col_vtracs:
                if vtrac and vtrac not in found_vtracs:
                    found_vtracs.append(vtrac)

        # Log results for this row
        if found_vtracs:
            logger.debug(
                f"Row processing found {len(found_vtracs)} VTRACs: {found_vtracs}")
            logger.debug(f"Article -> VTRAC mapping: {article_vtrac_map}")
        else:
            # Log articles that were searched but yielded no results
            articles = [row[col] for col in Config.ADDITIONAL_ARTICLE_COLS
                        if not pd.isna(row[col]) and isinstance(row[col], str) and row[col].strip() != '']
            if articles:
                logger.warning(f"No VTRACs found for articles: {articles}")
            else:
                logger.warning("Row has no valid articles to search")

        return found_vtracs

    def process_dataframe(self) -> pd.DataFrame:
        """
        Process entire input dataframe
        
        Returns:
            DataFrame with added VTRAC columns
        """
        logger.info("Reading input table...")

        try:
            # Read tabl_out with explicit string type casting
            tabl_out_df = pd.read_excel(
                self.tabl_out_path,
                dtype={col: str for col in Config.ADDITIONAL_ARTICLE_COLS},
                engine='openpyxl'
            )

            logger.info(f"Loaded input table with {len(tabl_out_df)} rows")

            # Log sample of input data
            sample_rows = min(5, len(tabl_out_df))
            logger.debug(f"Sample of input data (first {sample_rows} rows):")
            for idx, row in tabl_out_df.head(sample_rows).iterrows():
                article_values = {
                    col: row[col] for col in Config.ADDITIONAL_ARTICLE_COLS if not pd.isna(row[col])}
                logger.debug(f"Row {idx}: {article_values}")

            logger.info("Starting parallel VTRAC search...")

            # Store all row indices for progress tracking
            all_indices = list(range(len(tabl_out_df)))

            # Track rows with and without results
            rows_with_vtrac = set()
            rows_without_vtrac = set()

            # Parallel processing of rows
            with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
                # Submit processing jobs
                futures_dict = {
                    executor.submit(self.process_row, tabl_out_df.iloc[idx]): idx
                    for idx in all_indices
                }

                # Collect results with progress tracking
                results = [None] * len(tabl_out_df)

                with tqdm(total=len(futures_dict), desc="Processing Rows", unit="row") as pbar:
                    for future in as_completed(futures_dict):
                        idx = futures_dict[future]
                        try:
                            result = future.result()
                            results[idx] = result

                            # Track if we found any VTRACs
                            if result:
                                rows_with_vtrac.add(idx)
                            else:
                                rows_without_vtrac.add(idx)

                        except Exception as e:
                            logger.error(f"Error processing row {idx}: {e}")
                            results[idx] = []

                        pbar.update(1)

            # Log summary of results
            logger.info(f"Processing complete: found VTRACs for {len(rows_with_vtrac)} rows, "
                        f"no VTRACs for {len(rows_without_vtrac)} rows")

            if rows_without_vtrac:
                # Log some examples of rows without VTRACs
                sample_size = min(5, len(rows_without_vtrac))
                sample_rows = list(rows_without_vtrac)[:sample_size]
                logger.warning(f"Sample of rows without VTRACs:")
                for idx in sample_rows:
                    row = tabl_out_df.iloc[idx]
                    article_values = {
                        col: row[col] for col in Config.ADDITIONAL_ARTICLE_COLS if not pd.isna(row[col])}
                    logger.warning(f"Row {idx}: Articles = {article_values}")

            # Add VTRAC columns dynamically
            for i in range(Config.MAX_VTRAC_COLUMNS):
                col_name = f'vtrac_{i+1}'
                # Fill column with results, using None if no result for that index
                tabl_out_df[col_name] = [
                    r[i] if r and i < len(r) else None
                    for r in results
                ]

            return tabl_out_df

        except Exception as e:
            logger.error(f"Error processing dataframe: {e}")
            raise

    def run(self) -> None:
        """
        Main execution method
        """
        try:
            # Start timing the execution
            import time
            start_time = time.time()

            logger.info("VTRAC Finder started")

            # Validate input files exist
            if not os.path.exists(self.tabl_out_path):
                logger.error(f"Input file not found: {self.tabl_out_path}")
                return

            if not os.path.exists(self.catalog_path):
                logger.error(f"Input file not found: {self.catalog_path}")
                return

            # Process dataframe and save
            result_df = self.process_dataframe()

            # Count non-empty VTRAC results
            vtrac_counts = 0
            for i in range(Config.MAX_VTRAC_COLUMNS):
                col_name = f'vtrac_{i+1}'
                if col_name in result_df.columns:
                    non_empty = result_df[col_name].notna().sum()
                    logger.info(
                        f"Column {col_name} has {non_empty} non-empty values")
                    vtrac_counts += non_empty

            logger.info(f"Total VTRAC values found: {vtrac_counts}")

            logger.info("Saving output file...")
            result_df.to_excel(Config.OUTPUT_PATH, index=False)

            # Calculate execution time
            end_time = time.time()
            execution_time = end_time - start_time

            logger.info(
                f"Processing complete. Output saved to {Config.OUTPUT_PATH}")
            logger.info(f"Total execution time: {execution_time:.2f} seconds")

        except Exception as e:
            logger.error(f"Error during processing: {e}")
            raise


def main():
    """
    Entry point for the script
    """
    try:
        logger.info("=" * 50)
        logger.info("VTRAC Finder Script Started")
        logger.info("=" * 50)

        # Display configuration
        logger.info("Configuration:")
        logger.info(
            f"  Input tables: {Config.INPUT_PATH_TABL_OUT}, {Config.INPUT_PATH_CATALOG}")
        logger.info(f"  Output table: {Config.OUTPUT_PATH}")
        logger.info(f"  Parallel workers: {Config.MAX_WORKERS}")
        logger.info(f"  Max VTRAC columns: {Config.MAX_VTRAC_COLUMNS}")
        logger.info("-" * 50)

        # Create and run VTRAC finder
        vtrac_finder = VTRACFinder(
            Config.INPUT_PATH_TABL_OUT,
            Config.INPUT_PATH_CATALOG
        )
        vtrac_finder.run()

        logger.info("=" * 50)
        logger.info("VTRAC Finder Script Completed Successfully")
        logger.info("=" * 50)

    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        logger.error("=" * 50)
        logger.error("VTRAC Finder Script Failed")
        logger.error("=" * 50)


if __name__ == "__main__":
    main()
