"""Main orchestrator — walks vip2025/ directory, detects banks, parses files, outputs JSON."""

import os
import sys
import logging
from datetime import datetime

from .config import DATA_DIR, OUTPUT_DIR, LOG_DIR
from .models import ParseResult
from .file_reader import read_excel_file
from .detector import detect_parser
from .output import save_file_result, save_combined_output, save_parse_report

# Import all parsers so they register themselves
from .parsers import standard_18col, narodny, kaspi, otbasy, tengri
from .parsers import alatau, tsesnabank, al_hilal, kazkom
from .parsers import forte, bank_rbk, eurasian, kassa_nova, delta
from .parsers import bcc, kzi, nurbank, altyn
from .parsers import halyk_finance, citibank, bank_razvitiya
from .parsers import china_banks, zaman

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'parse.log'), encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger('bank_parser')


def process_file(filepath: str, folder_name: str) -> ParseResult:
    """Process a single bank statement file."""
    filename = os.path.basename(filepath)
    ext = os.path.splitext(filename)[1].lower()

    file_info = {
        'filepath': filepath,
        'filename': filename,
        'extension': ext,
        'folder_name': folder_name,
    }

    result = ParseResult(filepath=filepath, source_file=filename)

    # Skip non-Excel files
    if ext not in ('.xlsx', '.xls'):
        result.parse_status = 'skipped'
        result.errors.append(f'Unsupported extension: {ext}')
        return result

    # Read file
    try:
        sheets = read_excel_file(filepath)
    except Exception as e:
        result.parse_status = 'failed'
        result.errors.append(f'File read error: {e}')
        logger.error(f'Failed to read {filename}: {e}')
        return result

    if not sheets or all(s.num_rows == 0 for s in sheets):
        result.parse_status = 'skipped'
        result.warnings.append('Empty file')
        return result

    # Detect parser
    parser_cls = detect_parser(sheets, file_info)
    if parser_cls is None:
        result.parse_status = 'failed'
        result.errors.append('No parser detected')
        logger.warning(f'No parser for {filename} in {folder_name}')
        return result

    # Parse
    try:
        parser = parser_cls()
        result = parser.parse(sheets, file_info)
    except Exception as e:
        result.parse_status = 'failed'
        result.errors.append(f'Parse error: {e}')
        logger.error(f'Parse error for {filename}: {e}', exc_info=True)

    return result


def process_all(data_dir: str = None, output_dir: str = None):
    """Process all bank statement files in data directory."""
    data_dir = data_dir or DATA_DIR
    output_dir = output_dir or OUTPUT_DIR

    if not os.path.exists(data_dir):
        logger.error(f'Data directory not found: {data_dir}')
        return

    all_results = []
    total_files = 0
    success_count = 0

    # Walk through bank folders
    for bank_folder in sorted(os.listdir(data_dir)):
        bank_path = os.path.join(data_dir, bank_folder)
        if not os.path.isdir(bank_path) or bank_folder.startswith('.'):
            continue

        logger.info(f'\n{"="*60}')
        logger.info(f'Processing bank: {bank_folder}')
        logger.info(f'{"="*60}')

        for filename in sorted(os.listdir(bank_path)):
            if filename.startswith('~') or filename.startswith('.'):
                continue
            if not filename.endswith(('.xlsx', '.xls')):
                continue

            filepath = os.path.join(bank_path, filename)
            total_files += 1

            logger.info(f'  Processing: {filename}')
            result = process_file(filepath, bank_folder)
            all_results.append(result)

            if result.parse_status in ('success', 'partial'):
                success_count += 1
                logger.info(f'    -> {result.parse_status}: {result.total_transactions} transactions '
                          f'(parser: {result.parser_used})')
            else:
                logger.warning(f'    -> {result.parse_status}: {result.errors}')

            # Save individual file result
            try:
                save_file_result(result, output_dir)
            except Exception as e:
                logger.error(f'Failed to save result for {filename}: {e}')

    # Save combined output and report
    logger.info(f'\n{"="*60}')
    logger.info(f'SUMMARY')
    logger.info(f'{"="*60}')
    logger.info(f'Total files: {total_files}')
    logger.info(f'Successful: {success_count}')
    logger.info(f'Failed: {total_files - success_count}')

    total_transactions = sum(r.total_transactions for r in all_results)
    logger.info(f'Total transactions: {total_transactions}')

    try:
        save_combined_output(all_results, output_dir)
        report_path = save_parse_report(all_results, output_dir)
        logger.info(f'Report saved: {report_path}')
    except Exception as e:
        logger.error(f'Failed to save combined output: {e}')

    return all_results


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Parse Kazakh bank statements to unified JSON')
    parser.add_argument('--data-dir', default=DATA_DIR, help='Input directory with bank folders')
    parser.add_argument('--output-dir', default=OUTPUT_DIR, help='Output directory for JSON files')
    parser.add_argument('--file', help='Process a single file (provide full path)')
    args = parser.parse_args()

    if args.file:
        folder = os.path.basename(os.path.dirname(args.file))
        result = process_file(args.file, folder)
        print(f'Status: {result.parse_status}')
        print(f'Transactions: {result.total_transactions}')
        print(f'Parser: {result.parser_used}')
        if result.errors:
            print(f'Errors: {result.errors}')
        save_file_result(result, args.output_dir)
    else:
        process_all(args.data_dir, args.output_dir)
