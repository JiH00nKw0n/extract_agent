import asyncio
import functools
import json

import jsonlines

# fetch_single_categorized_output 함수를 임포트합니다.
from fetch import (
    _fetch_auditing_output,
    _fetch_classification_output,
    _fetch_extracted_output,
    _fetch_table_data_rowwise,
    _fetch_table_data_cellwise,
)
from tqdm.asyncio import tqdm
from utils import parse_html_table_to_csv


def async_limit(max_concurrent:int=2):
    semaphore = asyncio.Semaphore(max_concurrent)
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            async with semaphore:
                return await func(*args, **kwargs)
        return wrapper
    return decorator

# JSON 파일 읽고 JSONL로 변환하는 코드
# @async_limit(max_concurrent=2)
async def process_data(company_name: str, input_file: str, output_file: str, quarter: str):
    
    # JSON 파일 읽기
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # texts, lines는 더 이상 사용하지 않습니다.
    tasks = []
    original_indices_and_content = []

    # 1. 추출(Extraction) 작업 목록 생성
    for i, item in enumerate(data):
        if 'content' in item and "<table>" not in item['content']:
            # 각 태스크와 해당 태스크의 원본 인덱스 및 content(chunk)를 저장
            tasks.append(_fetch_extracted_output(company_name, item['content'], quarter))
            original_indices_and_content.append((i, item['content']))
        
    # 2. 추출 작업 병렬 실행 및 결과 수집
    all_extracted_results = await tqdm.gather(*tasks, desc="Fetching extracted output")
    
    # 두 단계로 테이블 데이터를 처리: 1) row별 metric 추출, 2) cell별 값 추출
    table_data = []
    # 1단계: 테이블에서 metric 리스트 추출
    row_extraction_tasks = []
    metrics_by_table_index = {}  # 테이블 인덱스 별 metrics 저장
    # INSERT_YOUR_CODE
    # input_file에서 .json 말고 .html로 읽은 다음에 table 태그 달린 것만 가져오고싶어

    # HTML 파일 읽기
    input_file = input_file.replace('.json', '.html')
    with open(input_file, 'r', encoding='utf-8') as f:
        html_content = f.read()

    # table 태그가 포함된 부분만 추출
    import re
    table_chunks = re.findall(r'(<table.*?>.*?</table>)', html_content, re.DOTALL | re.IGNORECASE)

    # data를 table 태그가 달린 부분만으로 재구성
    data = []
    for chunk in table_chunks:
        data.append({'content': chunk})
    for i, item in enumerate(data):
        if 'content' in item and "<table" in item['content']:
            parsed_csv = parse_html_table_to_csv(item['content'])
            temp_table_data = {
                "index": i,
                "reference": parsed_csv
            }
            row_extraction_tasks.append(_fetch_table_data_rowwise(temp_table_data, company_name, quarter))
            metrics_by_table_index[i] = []  # 빈 리스트로 초기화
    
    # Row 추출 작업 실행
    row_results = await tqdm.gather(*row_extraction_tasks, desc="Extracting metrics from tables")
    
    # 각 테이블에서 추출된 metrics를 테이블 인덱스 별로 저장
    for idx, metrics in enumerate(row_results):
        table_index = list(metrics_by_table_index.keys())[idx]
        metrics_by_table_index[table_index] = metrics
    
    # 2단계: 각 metric에 대해 cell 값 추출
    cell_extraction_tasks = []
    
    for table_idx, metrics in metrics_by_table_index.items():
        for metric in metrics:
            # 테이블 데이터에서 추출된 metric에 대해 cell 값 추출 작업 생성
            original_table_data = next((item['content'] for i, item in enumerate(data) if i == table_idx and 'content' in item), "")
            metric_with_reference = {
                **metric,
                "index": table_idx,
                "reference": parse_html_table_to_csv(original_table_data)
            }
            cell_extraction_tasks.append(_fetch_table_data_cellwise(metric_with_reference, company_name, quarter))
    
    # Cell 추출 작업 실행
    cell_results = await tqdm.gather(*cell_extraction_tasks, desc="Extracting values from table cells")
    
    # 모든 cell 결과를 하나의 리스트로 병합
    for cells in cell_results:
        table_data.extend(cells)
    
    # 3. 분류(Categorization) 작업 준비
    auditing_tasks = []
    extracted_data_for_merge = [] # 최종 병합을 위한 추출 데이터 저장

    # 추출 결과를 순회하며 분류 작업 생성
    for idx, batch_results in enumerate(all_extracted_results):
        original_index, original_content = original_indices_and_content[idx]
        for extracted_result in batch_results: # extracted_result는 단일 KPI 정보 딕셔너리
            # 분류 작업 추가
            task = _fetch_auditing_output(company_name, original_content, extracted_result, quarter)
            auditing_tasks.append(task)
            # 최종 병합을 위해 추출 결과와 원본 인덱스 저장
            extracted_data_for_merge.append({"_id": original_index, **extracted_result})

    print(len(all_extracted_results))
    # 4. 분류 작업 병렬 실행 및 결과 수집
    final_results = []
    auditing_results_list = await tqdm.gather(*auditing_tasks, desc="Fetching auditing output")
    print(len(auditing_results_list))
    auditing_output_file = output_file.replace('.jsonl', '_auditing.jsonl')
    with jsonlines.open(auditing_output_file, mode='w') as writer:
        # auditing 결과 저장
        for extracted_data, auditing_info in zip(extracted_data_for_merge, auditing_results_list):
            print(extracted_data.keys())
            print(auditing_info.keys())
            final_result = {**extracted_data, **auditing_info}
            final_results.append(final_result)
            print(final_result.keys())
            writer.write({
                "index": final_result['_id'],
                "title": final_result['title'],
                "value": final_result['value'],
                "unit": final_result['unit'],
                "period": final_result['period'],
                "type_": final_result['type_'],
                "reference": final_result['reference']
                })
        

    print(f"Auditing results saved to {auditing_output_file}")
    
    # 분류 작업 병렬 실행
    classification_tasks = [_fetch_classification_output(result) for result in final_results]
    classification_results = await tqdm.gather(*classification_tasks, desc="Fetching classification output")
    print(len(classification_results))
    classification_output_file = output_file.replace('.jsonl', '_classification.jsonl')
    with jsonlines.open(classification_output_file, mode='w') as writer:
        for result in classification_results:
            writer.write({
                "index": result['_id'],
                "category": result['category'],
                "title": result['title'],
                "value": result['value'],
                "unit": result['unit'],
                "period": result['period'],
                "type_": result['type_'],
                "reference": result['reference']
                })
        # 테이블 데이터 먼저 저장
        # Print table data for debugging
        print("Table data structure:", type(table_data))
        print("Table data length:", len(table_data))
        if len(table_data) > 0:
            print("First table item type:", type(table_data[0]))
            # Check if table_data is a list of lists or list of dicts
            if isinstance(table_data[0], list):
                for table_item in table_data:
                    for metric in table_item:
                        if isinstance(metric, dict):
                            writer.write(metric)
                        else:
                            print(f"Warning: Non-dict metric found: {type(metric)}")
            else:
                # If table_data is a flat list of metrics
                for metric in table_data:
                    if isinstance(metric, dict):
                        writer.write(metric)
                    else:
                        print(f"Warning: Non-dict metric found: {type(metric)}")

    print(f"Classification results saved to {classification_output_file}")

from pathlib import Path

base_path = str(Path(__file__).parent)
input_path = base_path + "/8-k_sample/2014Q4.json"
output_path_base = base_path + "/result/2014Q4_improved_results.jsonl"

quarter = "2014 4Q"
name = "chipotle"

asyncio.run(process_data(name, input_path, output_path_base, quarter))
