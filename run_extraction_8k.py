import asyncio
import json
from itertools import chain
from typing import List

import jsonlines
from tqdm.asyncio import tqdm

# fetch_single_categorized_output 함수를 임포트합니다.
from src import (
    DocType,
    _fetch_classification_output,
    _fetch_extracted_output,
    _fetch_table_data_cell_wise_output,
    _fetch_table_data_row_wise_output,
    check_valid_value,
    duplicate_token_count,
    extract_table_with_preceding_text,
    get_text_from_html,
    split_html,
)


def matched_chunk_with_html(html_chunks: List[str], chunk_content: str) -> str:
    """duplicate 개수가 가장 큰 chunk를 반환"""
    best_chunk = None
    max_duplicates = 0
    chunk_content_text = get_text_from_html(chunk_content)

    for chunk in html_chunks:
        chunk_text = get_text_from_html(chunk)
        duplicate_count = duplicate_token_count(chunk_text, chunk_content_text)
        if duplicate_count > max_duplicates:
            max_duplicates = duplicate_count
            best_chunk = chunk

    return best_chunk


async def process_data(company_name: str, parsed_file_path: str, raw_file_path: str, date: str, ticker: str):

    with open(raw_file_path, 'r', encoding='utf-8') as f:
        html_content = f.read()

    with open(parsed_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    quarter = f"{date[:4]} Q{(int(date[4:6]) - 1) // 3 + 1}"

    index_contents_dict = {}

    non_table_tasks = []
    table_tasks = []

    for i, item in enumerate(data):
        if 'content' in item and "<table>" not in item['content']:
            non_table_tasks.append((i, item['content']))
            index_contents_dict[i] = item["content"]

        elif 'content' in item and "<table>" in item['content']:
            table_tasks.append((i, item['content']))
            index_contents_dict[i] = item["content"]

    # 2. 추출 작업 병렬 실행 및 결과 수집
    all_extracted_results = await tqdm.gather(
        *[_fetch_extracted_output(company_name, c, quarter, DocType.FILING_8K) for _, c in non_table_tasks],
        desc="Fetching extracted output"
    )

    # 3. 분류 작업 및 결과 처리를 한 번에
    extracted_items = [
        (non_table_tasks[idx][0], extracted_result)
        for idx, batch_results in enumerate(all_extracted_results)
        for extracted_result in batch_results
    ]

    classification_results = await tqdm.gather(
        *[
            _fetch_classification_output(
                company_name, index_contents_dict[item_idx], extracted_result, quarter, DocType.FILING_8K
            )
            for item_idx, extracted_result in extracted_items
        ],
        desc="Fetching classification output"
    )

    # 4. 최종 결과 생성
    non_table_result = [
        {
            "index": item_idx,
            "category": classification_result['category'],
            "title": classification_result['title'],
            "value": check_valid_value(html_content, extracted_result['value']),
            "unit": classification_result['unit'],
            "period": classification_result['period'],
            "type_": classification_result['type_'],
            "reference": extracted_result['reference']
        }
        for (item_idx, extracted_result), classification_result
        in zip(extracted_items, classification_results)
    ]

    html_chunks = split_html(html_content)

    matched_table_html_chunks = [
        (idx, extract_table_with_preceding_text(matched_chunk_with_html(html_chunks, c))["content"]) for idx, c in
        table_tasks
    ]

    row_results = await tqdm.gather(
        *[_fetch_table_data_row_wise_output(
            {"index": idx, "reference": r}, company_name, quarter
        ) for idx, r in matched_table_html_chunks
        ],
        desc="Extracting metrics from tables"
    )

    cell_results = await tqdm.gather(
        *[_fetch_table_data_cell_wise_output(
            metrics, company_name, quarter
        ) for metrics in row_results
        ],
        desc="Extracting values from table cells"
    )

    table_result = list(chain.from_iterable(cell_results))

    output_file = f"./data/result/{ticker}_{quarter.replace(' ', '_')}_{date}_{parsed_file_path.split('/')[-1].replace('.json', '')}.jsonl"
    print(type(table_result))
    with jsonlines.open(output_file, mode='w') as writer:
        for result in non_table_result:
            writer.write(result)

        # Check if table_data is a list of lists or list of dicts
        if isinstance(table_result[0], list):
            for table_item in table_result:
                for metric in table_item:
                    if isinstance(metric, dict):
                        writer.write(metric)
                    else:
                        print(f"Warning: Non-dict metric found: {type(metric)}")
        else:
            # If table_data is a flat list of metrics
            for metric in table_result:
                if isinstance(metric, dict):
                    writer.write(metric)
                else:
                    print(f"Warning: Non-dict metric found: {type(metric)}")


if __name__ == "__main__":

    from pathlib import Path
    
    base_path = str(Path(__file__).parent)
    company_name = "Chipotle Mexican Grill, Inc."
    parsed_file_path = base_path + "/data/8-k_sample/chipotle/2024Q3.json"
    raw_file_path = base_path + "/data/8-k_sample/chipotle/2024Q3.html"
    date = "20240913"
    ticker = "CMG"

    asyncio.run(process_data(company_name, parsed_file_path, raw_file_path, date, ticker))
