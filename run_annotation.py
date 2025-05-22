import asyncio
import csv
import json
import os
from collections import defaultdict
from typing import Any, Dict, List, Tuple

from tqdm.asyncio import tqdm
import traceback

from fetch import _fetch_quote_relevance_output
from utils import get_chunk

import time
import datetime

# 로그 디렉토리 생성 함수
def ensure_log_directory(log_dir="logs"):
    """로그 디렉토리가 존재하는지 확인하고, 없으면 생성합니다."""
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    return log_dir

def init_timing_log(run_id, company_name, file_types):
    """타이밍 로그 초기화"""
    return {
        "run_id": run_id,
        "start_time": datetime.datetime.now().isoformat(),
        "company": company_name,
        "questions": [],
        "document_types": {doc_type: {"total_time": 0, "count": 0} for doc_type in file_types},
        "chunk_stats": {
            "total_chunks": 0,
            "processed_chunks": 0,
            "relevant_chunks": 0,
            "total_eval_time": 0,
            "avg_chunk_time": 0
        },
        "errors": [],
        "completion_time": None,
        "total_time": None
    }

# 타이밍 로그 저장 함수
def save_timing_log(timing_data, log_dir, run_id, company_name):
    """타이밍 로그를 저장합니다."""
    log_path = os.path.join(log_dir, f"timing_log_{company_name}_{run_id}.json")
    
    # 현재 시간 업데이트
    timing_data["last_updated"] = datetime.datetime.now().isoformat()
    
    with open(log_path, 'w', encoding='utf-8') as f:
        json.dump(timing_data, f, ensure_ascii=False, indent=2)
    
    return log_path

# 에러 로그 저장 함수
def save_error_log(timing_data, log_dir, run_id, company_name):
    """에러 로그를 저장합니다."""
    if not timing_data.get("errors"):
        return None
        
    error_log_path = os.path.join(log_dir, f"error_log_{company_name}_{run_id}.json")
    
    with open(error_log_path, 'w', encoding='utf-8') as f:
        json.dump({"errors": timing_data["errors"], "last_updated": datetime.datetime.now().isoformat()}, 
                 f, ensure_ascii=False, indent=2)
    
    return error_log_path

# 에러 로깅 함수
def log_error(timing_data, error_type, error_message, context=None, log_dir=None, run_id=None, company_name=None):
    """에러 정보를 로깅하고 즉시 저장합니다."""
    error_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "type": error_type,
        "message": error_message,
        "context": context or {}
    }
    
    if "errors" not in timing_data:
        timing_data["errors"] = []
        
    timing_data["errors"].append(error_entry)
    
    # 에러 발생 시 즉시 로그 저장 (선택적)
    if log_dir and run_id and company_name:
        save_timing_log(timing_data, log_dir, run_id, company_name)
        save_error_log(timing_data, log_dir, run_id, company_name)
        
    return error_entry
    
def should_process_chunk(chunk: str) -> bool:
    """
    명백히 관련 없는 청크를 건너뛰기 위한 사전 필터링
    """

    # 비어있는 청크 건너뛰기
    if not chunk or not chunk.strip():
        return False
    
    # 짧은 청크 건너뛰기 
    if len(chunk.strip()) < 10:
        return False
    
    if chunk.strip().isdigit():
        return False
    
    return True

async def process_chunk(question: str, chunk_idx: int, chunk: str, file_type: str, category: str, timing_data=None) -> Dict[str, Any]:
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
    if not should_process_chunk(chunk):
        return None

    # 5개 병렬로 실행
    evaluation_tasks = [
        _fetch_quote_relevance_output(question, chunk)
        for _ in range(5)
    ]
    evaluation_results = await asyncio.gather(*evaluation_tasks)
    
    relevant_count = sum(1 for result, _ in evaluation_results if result)
    
    if relevant_count == 0:
        return None
    
    relevance_score = 2 if relevant_count == 5 else (1 if relevant_count >=3 else 0)
    
    all_result_lines = set()
    all_translated_lines = set()

    primary_result = None
    primary_translated = None
    
    for result, translated in evaluation_results:
        if result:
            all_result_lines.update(result)
            all_translated_lines.update(translated)
            
            # Store the first non-empty result
            if primary_result is None:
                primary_result = result
                primary_translated = translated
    
    combined_result_lines = list(all_result_lines)
    combined_translated_lines = list(all_translated_lines)
    
    result_obj = {
        "category": category,
        "question": question,
        "file_type": file_type,
        "index": chunk_idx,
        "relevance_count": relevant_count,
        "relevance_score": relevance_score,
        "result_lines": combined_result_lines,
        "translated_lines": combined_translated_lines,
        "chunk": chunk
    }
    
    return result_obj


pairs: List[Tuple[str, str]] = []
csv_file_path = "annotations/sbux/starbucks.csv"

try:
    with open(csv_file_path, 'r', encoding='cp949') as f:
        reader = csv.reader(f)
        next(reader)  # 헤더 행 건너뛰기 (만약 있다면)
        for row in reader:
            if len(row) == 2:  # 정확히 두 개의 열이 있는지 확인
                pairs.append((row[0], row[1]))
except Exception as e:
    print(f"CSV 파일 로드 중 오류 발생: {e}")
    traceback.print_exc()
    pairs = []

print(pairs)
category_dict = defaultdict(int)

async def main():
    # 로그 디렉토리 설정
    log_dir = ensure_log_directory("logs")
    company_name = "sbux"
    run_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 타이밍 로그 초기화
    file_types = [
        "Earnings",
        "10-K",
        "10-Q",
        "8-K",
        "DEF14A"
    ]
    
    timing_data = init_timing_log(run_id, company_name, file_types)
    timing_log_path = save_timing_log(timing_data, log_dir, run_id, company_name)
    
    print(f"로그 저장 위치: {log_dir}")
    print(f"타이밍 로그 파일: {os.path.basename(timing_log_path)}")
    
    overall_start_time = time.time()
    
    try:
        # 각 질문 처리
        for question_idx, (category, question) in enumerate(pairs):
            question_start_time = time.time()
            
            # 질문 데이터 초기화
            question_data = {
                "idx": question_idx + 1,
                "category": category,
                "question": question,
                "start_time": datetime.datetime.now().isoformat(),
                "documents": [],
                "total_chunks": 0,
                "relevant_chunks": 0,
                "time_seconds": 0
            }
            
            print(f"진행 상황: {question_idx+1}/{len(pairs)} 질문 처리 중...")
            
            try:
                file_names = [
                    "earnings.html",
                    "10-k.html",
                    "10-q.html",
                    "8-k.json",
                    "def14a.json"
                ]
                
                os.makedirs(f"./annotations/{company_name}/qa/{category.replace(' ', '_')}", exist_ok=True)
                
                print(f"질문 처리 중: {question}")
                
                _category = category.replace(' ', '_')
                category_dict[_category] += 1
                print(f"카테고리 인덱스: {category_dict[_category]}")
                
                # 각 파일 타입 처리
                for file_type, file_name in zip(file_types, file_names):
                    doc_start_time = time.time()
                    
                    # 문서 데이터 초기화
                    doc_data = {
                        "file_type": file_type,
                        "start_time": datetime.datetime.now().isoformat(),
                        "total_chunks": 0,
                        "filtered_chunks": 0,
                        "relevant_chunks": 0,
                        "chunk_times": [],
                        "time_seconds": 0,
                        "errors": []
                    }
                    
                    try:
                        # 출력 파일 경로 설정
                        file_path = f"annotations/{company_name}/{file_name}"
                        
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
                            error_msg = f"잘못된 파일 타입: {file_type}"
                            log_error(timing_data, "invalid_file_type", error_msg, None, 
                                      log_dir, run_id, company_name)
                            raise ValueError(error_msg)
                        
                        # 파일 내용 로드
                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                file_content = f.read()
                        except Exception as e:
                            error_context = {"file_path": file_path, "file_type": file_type}
                            error_msg = f"파일 로드 중 오류: {str(e)}"
                            log_error(timing_data, "file_load_error", error_msg, error_context, 
                                      log_dir, run_id, company_name)
                            doc_data["errors"].append({"type": "file_load_error", "message": error_msg})
                            print(error_msg)
                            continue
                        
                        # 청크 생성
                        chunks = get_chunk(file_content, file_type)
                        doc_data["total_chunks"] = len(chunks)
                        question_data["total_chunks"] += len(chunks)
                        timing_data["chunk_stats"]["total_chunks"] += len(chunks)
                        
                        print(f"총 {len(chunks)}개의 청크를 처리합니다...")
                        
                        # 모든 청크에 대한 태스크 생성
                        tasks = []
                        for chunk_idx, chunk in chunks.items():
                            task = process_chunk(question, chunk_idx, chunk, file_type, category, timing_data)
                            tasks.append(task)
                        
                        # tqdm을 사용하여 진행 상황을 표시하면서 병렬로 모든 태스크 실행
                        results = await tqdm.gather(*tasks, desc="청크 처리 중", leave=False)
                        
                        # 결과 필터링 (None이 아닌 결과만 유지)
                        valid_results = [result for result in results if result is not None]
                        doc_data["filtered_chunks"] = len(valid_results)
                        timing_data["chunk_stats"]["processed_chunks"] += len(valid_results)
                        
                        # 처리 시간 정보 수집
                        for result in valid_results:
                            if "processing_time" in result:
                                doc_data["chunk_times"].append(result["processing_time"])
                                
                            if "eval_time" in result:
                                timing_data["chunk_stats"]["total_eval_time"] += result.get("eval_time", 0)
                        
                        # 관련성 점수 1 이상인 결과만 선택
                        final_results = [result for result in valid_results if result["relevance_score"] >= 1]
                        doc_data["relevant_chunks"] = len(final_results)
                        question_data["relevant_chunks"] += len(final_results)
                        timing_data["chunk_stats"]["relevant_chunks"] += len(final_results)
                        
                        print(f"\n총 {len(final_results)}개의 관련 청크를 찾았습니다.")
                        
                        # 타이밍 정보 제거
                        output_results = []
                        for result in final_results:
                            result_copy = result.copy()
                            if "processing_time" in result_copy:
                                del result_copy["processing_time"]
                            if "eval_time" in result_copy:
                                del result_copy["eval_time"]
                            output_results.append(result_copy)
                        
                        # JSONL 파일 생성
                        with open(output_file, 'w', encoding='utf-8') as jsonl_file:
                            # tqdm을 사용하여 저장 과정도 진행 상황 표시
                            for result in tqdm(output_results, desc="결과 저장 중"):
                                # 결과를 JSONL 라인으로 작성
                                jsonl_file.write(json.dumps(result, ensure_ascii=False) + '\n')
                        
                        print(f"\n결과가 {output_file}에 저장되었습니다.")
                        print(f"파일에는 {len(final_results)}개의 청크 관련 결과가 포함되어 있습니다.")
                    
                    except Exception as e:
                        # 문서 처리 중 오류
                        error_context = {
                            "file_type": file_type, 
                            "file_name": file_name,
                            "category": category,
                            "question": question
                        }
                        error_msg = f"{file_type} 문서 처리 중 오류: {str(e)}"
                        log_error(timing_data, "document_processing_error", error_msg, error_context,
                                 log_dir, run_id, company_name)
                        doc_data["errors"].append({"type": "document_processing_error", "message": error_msg})
                        print(error_msg)
                        traceback.print_exc()
                    
                    finally:
                        # 문서 처리 완료 시간 계산
                        doc_time = time.time() - doc_start_time
                        doc_data["time_seconds"] = doc_time
                        doc_data["end_time"] = datetime.datetime.now().isoformat()
                        
                        # 문서 타입 통계 업데이트
                        timing_data["document_types"][file_type]["total_time"] += doc_time
                        timing_data["document_types"][file_type]["count"] += 1
                        
                        # 질문 데이터에 문서 데이터 추가
                        question_data["documents"].append(doc_data)
                        
                        print(f"{file_type} 처리 시간: {doc_time:.2f}초")
                        
                        # 문서 처리 후 로그 저장
                        save_timing_log(timing_data, log_dir, run_id, company_name)
                
            except Exception as e:
                # 질문 처리 중 오류
                error_context = {"category": category, "question": question, "question_idx": question_idx}
                error_msg = f"질문 처리 중 오류: {str(e)}"
                log_error(timing_data, "question_processing_error", error_msg, error_context,
                         log_dir, run_id, company_name)
                print(error_msg)
                traceback.print_exc()
            
            finally:
                # 질문 처리 완료 시간 계산
                question_time = time.time() - question_start_time
                question_data["time_seconds"] = question_time
                question_data["end_time"] = datetime.datetime.now().isoformat()
                
                # 타이밍 데이터에 질문 데이터 추가
                timing_data["questions"].append(question_data)
                
                print(f"질문 처리 시간: {question_time:.2f}초 ({question_time/60:.2f}분)")
                
                # 질문 처리 후 로그 저장
                save_timing_log(timing_data, log_dir, run_id, company_name)
                if timing_data.get("errors"):
                    save_error_log(timing_data, log_dir, run_id, company_name)
    
    except Exception as e:
        # 전체 처리 중 오류
        error_msg = f"전체 처리 중 오류: {str(e)}"
        log_error(timing_data, "overall_processing_error", error_msg, None,
                 log_dir, run_id, company_name)
        print(error_msg)
        traceback.print_exc()
    
    finally:
        # 최종 통계 계산
        total_time = time.time() - overall_start_time
        timing_data["completion_time"] = datetime.datetime.now().isoformat()
        timing_data["total_time"] = total_time
        
        # 평균 계산
        if timing_data["chunk_stats"]["processed_chunks"] > 0:
            timing_data["chunk_stats"]["avg_chunk_time"] = timing_data["chunk_stats"]["total_eval_time"] / timing_data["chunk_stats"]["processed_chunks"]
        
        # 요약 통계 추가
        timing_data["summary"] = {
            "total_questions": len(pairs),
            "avg_time_per_question": total_time / len(pairs) if pairs else 0,
            "hours": int(total_time // 3600),
            "minutes": int((total_time % 3600) // 60),
            "seconds": total_time % 60
        }
        
        # 최종 타이밍 데이터 저장
        final_timing_path = save_timing_log(timing_data, log_dir, run_id, company_name)
        final_error_path = None
        if timing_data.get("errors"):
            final_error_path = save_error_log(timing_data, log_dir, run_id, company_name)
        
        # 요약 출력
        print("\n" + "="*50)
        hours = timing_data["summary"]["hours"]
        minutes = timing_data["summary"]["minutes"]
        seconds = timing_data["summary"]["seconds"]
        print(f"처리 완료: {hours}시간 {minutes}분 {seconds:.2f}초")
        print(f"처리된 총 청크 수: {timing_data['chunk_stats']['total_chunks']}개")
        print(f"관련 청크 수: {timing_data['chunk_stats']['relevant_chunks']}개")
        if timing_data["chunk_stats"]["processed_chunks"] > 0:
            print(f"청크당 평균 처리 시간: {timing_data['chunk_stats']['avg_chunk_time']:.4f}초")
        print(f"오류 발생 수: {len(timing_data.get('errors', []))}개")
        print(f"타이밍 로그 저장 위치: {final_timing_path}")
        if final_error_path:
            print(f"에러 로그 저장 위치: {final_error_path}")
        print("="*50)

if __name__ == "__main__":
    asyncio.run(main())