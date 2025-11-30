import requests
import csv
import os

# 발급받은 API 키를 여기에 입력하세요. (실제 사용 시 대체 필요)
SERVICE_KEY = "7443754663506a653435786f4c7478"
# 저장할 디렉토리 설정
OUTPUT_DIR = "experiment/openapi" 
# OUTPUT_DIR이 없으면 생성
os.makedirs(OUTPUT_DIR, exist_ok=True) 

# --- API 공통 설정 ---
BASE_URL = "http://t-data.seoul.go.kr/apig/apiman-gateway/tapi/"
PAGE_SIZE = 1000 # 한 페이지당 가져올 데이터 수 (API 제한에 따라 조정 가능)

def fetch_and_save_data(api_name, csv_filename, headers, data_key, base_params, max_page=None):
    """
    OpenAPI에서 데이터를 반복적으로 가져와 CSV 파일로 저장하는 일반 함수
    
    Args:
        api_name (str): API 엔드포인트 경로
        csv_filename (str): 저장할 CSV 파일 이름
        headers (list): CSV 파일의 제목줄 (컬럼 이름)
        data_key (str): 응답 JSON에서 실제 데이터 배열을 포함하는 키
        base_params (dict): API 호출의 기본 파라미터
        max_page (int, optional): 테스트용 최대 페이지 수. None이면 전체 데이터 수집.
    """
    print(f"--- {api_name} 데이터 수집 시작 ---")
    
    data_list = []
    page = 1
    total_count = 0
    max_retries = 3

    while True:
        if max_page is not None and page > max_page:
            break
            
        params = base_params.copy()
        params['page'] = page
        
        url = BASE_URL + api_name
        
        print(f"요청 중: 페이지 {page}...")
        
        try_count = 0
        success = False
        while try_count < max_retries:
            try:
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status() # HTTP 오류 발생 시 예외 발생
                data = response.json()
                success = True
                break
            except requests.exceptions.RequestException as e:
                try_count += 1
                print(f"오류 발생 (재시도 {try_count}/{max_retries}): {e}")
                if try_count == max_retries:
                    print(f"구간(또는 지점) 데이터를 가져오는데 실패했습니다. (API: {api_name}, 페이지: {page})")
                    return # 실패 시 종료

        if not success:
            continue
            
        # 데이터 추출
        current_data = data.get(data_key)
        
        # 첫 페이지에서만 total_count 확인
        if page == 1:
            total_count = data.get('totalCount', len(current_data) if current_data else 0)
            if total_count == 0:
                print("수집할 데이터가 없습니다.")
                break
            print(f"총 데이터 수: {total_count}개")
        
        if not current_data:
            break # 데이터가 더 이상 없으면 종료

        data_list.extend(current_data)
        
        if len(data_list) >= total_count:
            break # 전체 데이터 수를 넘으면 종료
            
        page += 1

    # CSV 파일로 저장
    full_path = os.path.join(OUTPUT_DIR, csv_filename)
    try:
        with open(full_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            
            # 필드에 포함된 데이터만 필터링하여 저장
            filtered_data = []
            for item in data_list:
                filtered_item = {header: item.get(header) for header in headers}
                filtered_data.append(filtered_item)
            
            writer.writerows(filtered_data)
        
        print(f"✅ 데이터 수집 완료. 총 {len(data_list)}개 항목이 '{full_path}'에 저장되었습니다.")
        
    except IOError as e:
        print(f"파일 저장 중 오류 발생: {e}")

# 1. 지점 데이터 (Node Data) 수집
node_headers = ['nodeId', 'nodeName', 'y', 'x']
node_base_params = {
    'authKey': SERVICE_KEY,
    'pageSize': PAGE_SIZE,
    'page': 1 # 시작 페이지
}
fetch_and_save_data(
    api_name='TopisIccMsNode/1.0',
    csv_filename='spot_data.csv',
    headers=node_headers,
    data_key='TopisIccMsNode', # 응답 JSON에서 데이터 배열의 키
    base_params=node_base_params
)

print("\n" + "="*50 + "\n")

# 2. 구간 데이터 (Link Data) 수집
link_headers = ['linkID', 'stnodeID', 'ednodeID', 'mapDist']
link_base_params = {
    'authKey': SERVICE_KEY,
    'pageSize': PAGE_SIZE,
    'page': 1 # 시작 페이지
}
fetch_and_save_data(
    api_name='TopisIccMsLink/1.0',
    csv_filename='section_data.csv',
    headers=link_headers,
    data_key='TopisIccMsLink', # 응답 JSON에서 데이터 배열의 키
    base_params=link_base_params
)
