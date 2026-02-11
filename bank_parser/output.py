"""JSON output serialization."""

import json
import os
import logging
from typing import List
from datetime import datetime

from .models import ParseResult
from .config import OUTPUT_DIR

logger = logging.getLogger('bank_parser')


def save_file_result(result: ParseResult, output_dir: str = None) -> str:
    """Save parse result for a single file as JSON.

    Returns path to saved file.
    """
    out_dir = output_dir or OUTPUT_DIR
    os.makedirs(out_dir, exist_ok=True)

    # Create filename from source file
    base = os.path.splitext(result.source_file)[0]
    # Sanitize filename
    safe_name = "".join(c if c.isalnum() or c in '-_.' else '_' for c in base)
    out_path = os.path.join(out_dir, f"{safe_name}.json")

    data = result.to_dict()
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved {result.total_transactions} transactions to {out_path}")
    return out_path


def save_combined_output(results: List[ParseResult], output_dir: str = None) -> str:
    """Save all transactions from all files into one combined JSON."""
    out_dir = output_dir or OUTPUT_DIR
    os.makedirs(out_dir, exist_ok=True)

    all_transactions = []
    for r in results:
        all_transactions.extend([t.to_dict() for t in r.transactions])

    out_path = os.path.join(out_dir, 'all_transactions.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(all_transactions, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved {len(all_transactions)} total transactions to {out_path}")
    return out_path


def save_parse_report(results: List[ParseResult], output_dir: str = None) -> str:
    """Save summary report of all parsing results."""
    out_dir = output_dir or OUTPUT_DIR
    os.makedirs(out_dir, exist_ok=True)

    summary = {
        'generated_at': datetime.now().isoformat(),
        'total_files': len(results),
        'status_counts': {},
        'total_transactions': 0,
        'files': [],
    }

    for r in results:
        summary['status_counts'][r.parse_status] = summary['status_counts'].get(r.parse_status, 0) + 1
        summary['total_transactions'] += r.total_transactions
        summary['files'].append({
            'source_file': r.source_file,
            'bank_detected': r.bank_detected,
            'parser_used': r.parser_used,
            'parse_status': r.parse_status,
            'total_transactions': r.total_transactions,
            'errors': r.errors,
            'warnings': r.warnings[:5],  # Limit warnings in report
        })

    out_path = os.path.join(out_dir, 'parse_report.json')
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    logger.info(f"Parse report saved to {out_path}")
    return out_path
