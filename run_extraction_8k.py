import asyncio
import functools
import json

import jsonlines

# fetch_single_categorized_output 함수를 임포트합니다.
from fetch import (
    _fetch_auditing_output,
    _fetch_classification_output,
    _fetch_extracted_output,
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
    
    # 테이블 데이터를 auditing 데이터와 함께 저장하기 위한 리스트
    table_data = []
    for i, item in enumerate(data):
        if 'content' in item and "<table>" in item['content']:
            parsed_csv = parse_html_table_to_csv(item['content'])
            table_data.append({
                "index": i,
                "title": "table",
                "value": parsed_csv,
                "unit": "N/A",
                "period": "N/A",
                "type_": "actual",
                "category": "financials",
                "reference": item['content']
            })

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
        for table_item in table_data:
            writer.write(table_item)

    print(f"Classification results saved to {classification_output_file}")

input_path = "/Users/junekwon/Desktop/Projects/extraction_agent/8-k_sample/inseego.json"
output_path_base = "./result/inseego_improved_results.jsonl" 

quarter = "2020 4Q"
name = "Inseego"

asyncio.run(process_data(name, input_path, output_path_base, quarter))
