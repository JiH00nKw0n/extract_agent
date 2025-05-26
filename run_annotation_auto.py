import asyncio
import csv
import json
import os
from collections import defaultdict
from typing import Any, Dict, List, Tuple

from tqdm import tqdm
from tqdm.asyncio import tqdm as tqdm_asyncio

from fetch import _fetch_chunk_relevance_output, _fetch_quote_relevance_output
from utils import get_chunk


async def process_chunk(question: str, chunk_idx: int, chunk: str, file_type: str, category: str) -> Dict[str, Any]:
    """
    각 청크를 처리하는 코루틴 함수입니다.
    
    Args:
        question: 질문 텍스트
        chunk_idx: 청크 인덱스
        chunk: 청크 텍스트
        file_type: 파일 타입
        
    Returns:
        결과 객체 또는 None (결과가 없는 경우)
    """
    result_lines, translated_lines = await _fetch_quote_relevance_output(question, chunk)
    
    if result_lines:  # 결과가 있는 경우에만 반환
        # 결과 객체 생성
        result_obj = {
            "category": category,
            "question": question,
            "file_type": file_type,
            "index": chunk_idx,
            "result_lines": result_lines,
            "translated_lines": translated_lines,
            "chunk": chunk
        }
        return result_obj
    
    return None  # 결과가 없는 경우

async def process_chunk_five_times(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    각 청크를 처리하는 코루틴 함수입니다.
    
    Args:
        question: 질문 텍스트
        chunk_idx: 청크 인덱스
        chunk: 청크 텍스트
        file_type: 파일 타입
        
    Returns:
        결과 객체 또는 None (결과가 없는 경우)
    """
    
    results = await tqdm_asyncio.gather(*[
        _fetch_chunk_relevance_output(data["question"], data["chunk"])
        for _ in range(5)
    ])
    
    num_true = sum(results)
    
    result_obj = {
        "relevance_score": num_true,
        **data,
    }
    
    return result_obj


pairs: List[Tuple[str, str]] = []
csv_file_path = "./annotations/sbux/starbucks_IM.csv"

with open(csv_file_path, 'r', encoding='cp949') as f:
    reader = csv.reader(f)
    next(reader) # 헤더 행 건너뛰기 (만약 있다면)
    for row in reader:
        if len(row) == 2: # 정확히 두 개의 열이 있는지 확인
            pairs.append((row[0], row[1]))

print(pairs)
category_dict = defaultdict(int)

# async def process_category_question_pair(category, question):
#     file_types = [
#         "Earnings",
#         "10-K",
#         "10-Q",
#         "8-K",
#         "DEF14A"
#     ]
#     file_names = [
#         "earnings.html",
#         "10-k.html",
#         "10-q.html",
#         "8-k.json",
#         "def14a.json"
#     ]
    
#     company_name = "sbux"
#     _category = category.replace(' ', '_')
    
#     os.makedirs(f"./annotations/{company_name}/qa/{_category}", exist_ok=True)

#     print(question)
#     print(category_dict[category])
#     category_dict[_category] += 1

#     for file_type, file_name in zip(file_types, file_names):
#         file_path = f"/Users/junekwon/Desktop/Projects/extraction_agent/annotations/{company_name}/{file_name}"

#         output_file = f"./annotations/{company_name}/qa/{_category}/relevance_results_{file_type.lower().replace('-', '').replace(' ', '')}_filter_q{category_dict[_category]}.jsonl"

#         with open(file_path, 'r', encoding='utf-8') as f:
#             file_content = f.read()

#         chunks = get_chunk(file_content, file_type)

#         print(f"[{file_type}] 총 {len(chunks)}개의 청크를 처리합니다...")

#         tasks = [
#             process_chunk(question, chunk_idx, chunk, file_type, category)
#             for chunk_idx, chunk in chunks.items()
#         ]

#         results = await tqdm_asyncio.gather(*tasks, desc=f"{file_type} 청크 처리 중", leave=False)
#         valid_results = [result for result in results if result is not None]

#         five_times_tasks = [process_chunk_five_times(result) for result in valid_results]
#         five_times_results = await tqdm_asyncio.gather(*five_times_tasks, desc="5번 처리 중", leave=False)

#         print(f"\n총 {len(valid_results)}개의 관련 청크를 찾았습니다.")

#         with open(output_file, 'w', encoding='utf-8') as jsonl_file:
#             for result in tqdm(valid_results, desc="결과 저장 중"):
#                 jsonl_file.write(json.dumps(result, ensure_ascii=False) + '\n')

#         print(f"\n결과가 {output_file}에 저장되었습니다.")
#         print(f"파일에는 {len(valid_results)}개의 청크 관련 결과가 포함되어 있습니다.")

# async def main():
#     tasks = [
#         process_category_question_pair(category, question)
#         for category, question in pairs
#     ]
#     await asyncio.gather(*tasks)
    
async def main():
    for category, question in pairs:
        file_types = [
            "Earnings",
            "10-K",
            "10-Q",
            "8-K",
            "DEF14A"
            ]
        file_names = [
            "earnings.html",
            "10-k.html",
            "10-q.html",
            "8-k.json",
            "def14a.json"
            ]
        
        company_name = "sbux"
        
        os.makedirs(f"./annotations/{company_name}/qa/{category.replace(' ', '_')}", exist_ok=True)
        
        print(question)
        print(category_dict[category])
        
        _category = category.replace(' ', '_')
        category_dict[_category] += 1
        
        for file_type, file_name in zip(file_types, file_names):
            
            file_path = f"/Users/junekwon/Desktop/Projects/extraction_agent/annotations/{company_name}/{file_name}"
            
            if file_type == "Earnings":
                output_file = f"./annotations/{company_name}/qa/{_category}/relevance_results_earnings_filter_q{category_dict[_category]}.jsonl"
            elif file_type == "10-K":
                output_file = f"./annotations/{company_name}/qa/{_category}/relevance_results_10k_filter_q{category_dict[_category]}.jsonl"
            elif file_type == "10-Q":
                output_file = f"./annotations/{company_name}/qa/{_category}/relevance_results_10q_filter_q{category_dict[_category]}.jsonl"
            elif file_type == "8-K":
                output_file = f"./annotations/{company_name}/qa/{_category}/relevance_results_8k_filter_q{category_dict[_category]}.jsonl"
            elif file_type == "DEF14A":
                output_file = f"./annotations/{company_name}/qa/{_category}/relevance_results_def14a_filter_q{category_dict[_category]}.jsonl"
            else:
                raise ValueError(f"Invalid file type: {file_type}")
            # 파일 내용 로드
            with open(file_path, 'r', encoding='utf-8') as f:
                file_content = f.read()
            
            chunks = get_chunk(file_content, file_type)
            
            print(f"총 {len(chunks)}개의 청크를 처리합니다...")
            
            # 모든 청크에 대한 태스크 생성
            tasks = [
                process_chunk(question, chunk_idx, chunk, file_type, category)
                for chunk_idx, chunk in chunks.items()
            ]
            
            # tqdm을 사용하여 진행 상황을 표시하면서 병렬로 모든 태스크 실행
            results = await tqdm_asyncio.gather(*tasks, desc="청크 처리 중", leave=False)
            
            # 결과 필터링 (None이 아닌 결과만 유지)
            valid_results = [result for result in results if result is not None]
            
            five_times_tasks = [
                process_chunk_five_times(result)
                for result in valid_results
            ]
            
            five_times_results = await tqdm_asyncio.gather(*five_times_tasks, desc="5번 처리 중", leave=False)
            
            
            print(f"\n총 {len(valid_results)}개의 관련 청크를 찾았습니다.")
            
            # JSONL 파일 생성
            with open(output_file, 'w', encoding='utf-8') as jsonl_file:
                # tqdm을 사용하여 저장 과정도 진행 상황 표시
                for result in tqdm(five_times_results, desc="결과 저장 중"):
                    # 결과를 JSONL 라인으로 작성
                    jsonl_file.write(json.dumps(result, ensure_ascii=False) + '\n')
            
            print(f"\n결과가 {output_file}에 저장되었습니다.")
            print(f"파일에는 {len(valid_results)}개의 청크 관련 결과가 포함되어 있습니다.")


if __name__ == "__main__":
    asyncio.run(main())