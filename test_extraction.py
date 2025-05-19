import os

from utils import get_chunk


def test_file(file_path: str, file_type: str, max_chunks: int = 5) -> None:
    """
    파일을 읽고 청킹한 후 결과를 출력합니다.
    
    Args:
        file_path: 파일 경로
        file_type: 파일 타입
        max_chunks: 출력할 최대 청크 수
    """
    print(f"\n{'='*50}")
    print(f"테스트: {file_type} - {os.path.basename(file_path)}")
    print(f"{'='*50}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        chunks = get_chunk(content, file_type)
        
        print(type(chunks))
        print(chunks[0])
        print(f"총 청크 수: {len(chunks)}")
        for idx, chunk in chunks.items():
            print(type(chunk))
        if len(chunks) > max_chunks:
            print(f"\n... 총 {len(chunks)}개 중 {max_chunks}개만 표시했습니다.")
    
    except Exception as e:
        print(f"오류 발생: {e}")


def main():
    # 파일 경로 설정 (aapl 폴더 경로)
    base_dir = "/Users/junekwon/Desktop/Projects/extraction_agent/aapl"
    
    # 각 파일 유형 테스트
    test_file(os.path.join(base_dir, "10-k.html"), "10-K")
    
    print("\n모든 테스트가 완료되었습니다.")


if __name__ == "__main__":
    main()