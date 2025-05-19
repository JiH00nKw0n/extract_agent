import csv

# text.txt 파일 내용을 읽어옵니다.
try:
    with open('misc/text.txt', 'r', encoding='utf-8') as f:
        lines = f.readlines()
except FileNotFoundError:
    print("Error: text.txt 파일을 찾을 수 없습니다.")
    exit()
except Exception as e:
    print(f"Error reading file: {e}")
    exit()

# CSV 데이터를 저장할 리스트
data = []
current_category = None
lines_per_category = 11  # 카테고리 1줄 + 질문 10줄

# 라인들을 11줄 단위로 처리
for i in range(0, len(lines), lines_per_category):
    # 현재 블록의 라인들을 가져옵니다.
    block = [line.strip() for line in lines[i: i + lines_per_category] if line.strip()]

    if not block:
        continue

    # 첫 번째 줄을 카테고리로 설정
    current_category = block[0]

    # 나머지 줄(최대 10개)을 질문으로 처리
    # 파일 끝 부분에서 10개 미만의 질문이 있을 수 있음
    for question_line in block[1:]:
        data.append({'category': current_category, 'question': question_line})

# CSV 파일 작성
output_filename = 'misc/output.csv'
try:
    with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['category', 'question']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(data)
    print(f"Successfully created {output_filename} based on 1 category + 10 questions pattern.")

except IOError:
    print(f"Error writing to file {output_filename}")
except Exception as e:
    print(f"An error occurred during CSV writing: {e}")
