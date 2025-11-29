import requests
import csv
import json
import time

# --- 환경 설정 및 상수 정의 ---
# 실제 API 키로 대체해야 합니다.
API_KEY = "YOUR_SEOUL_OPENAPI_KEY"

# API 엔드포인트 정의
# Note: 실제 API에서는 Base URL과 Path가 다를 수 있으나, 요구사항에 명시된 URL을 기준으로 구조화합니다.
NODE_URL = "http://t-data.seoul.go.kr/apig/apiman-gateway/tapi/TopisIccMsNode/1.0"
LINK_URL = "http://t-data.seoul.go.kr/apig/apiman-gateway/tapi/TopisIccMsLink/1.0"

# API 요청 시 페이지당 가져올 데이터 건수 (API에 따라 다를 수 있음)
PAGE_SIZE = 1000 
MAX_RETRIES = 3

# --- 목업 데이터 함수 정의 (실제 API 호출 대체) ---
# 실제 환경에서는 이 함수들을 requests.get을 사용한 실제 API 호출 로직으로 변경해야 합니다.
def get_mock_response(api_type, pageNo):
    """
    OpenAPI 응답 구조를 모방한 목업 데이터를 반환합니다.
    실제 API에서는 totalCount를 기반으로 반복 요청합니다.
    """
    total_count = 3500 # 총 3500개의 데이터가 있다고 가정
    is_last_page = (pageNo * PAGE_SIZE >= total_count)
    current_count = total_count - (pageNo - 1) * PAGE_SIZE
    if current_count > PAGE_SIZE:
        current_count = PAGE_SIZE
    elif current_count <= 0:
        return None

    if api_type == 'node':
        # 지점 데이터 목업 (nodeId, y, x)
        data_list = []
        for i in range(current_count):
            idx = (pageNo - 1) * PAGE_SIZE + i
            data_list.append({
                'nodeId': f'10000{idx:05d}',
                'nodeName': f'지점_{idx}',
                'y': 37.5000000 + idx * 0.00001,  # 위도
                'x': 127.0000000 + idx * 0.00001,  # 경도
                'grs80tm_x': 200000 + idx, 
                'grs80tm_y': 500000 + idx
            })
        return {
            'header': {'resultCode': '00', 'resultMsg': 'NORMAL SERVICE'},
            'nodeList': data_list,
            'totalCount': total_count,
            'pageNo': pageNo
        }
    
    elif api_type == 'link':
        # 구간 데이터 목업 (linkID, stnodeID, ednodeID, mapDist)
        data_list = []
        for i in range(current_count):
            idx = (pageNo - 1) * PAGE_SIZE + i
            data_list.append({
                'linkID': f'23300{idx:05d}',
                'stnodeID': f'10000{idx:05d}',
                'ednodeID': f'10000{idx+1:05d}',
                'mapDist': 500.0 + (idx % 100) * 1.5,
                'maxSpd': 60
            })
        return {
            'header': {'resultCode': '00', 'resultMsg': 'NORMAL SERVICE'},
            'linkList': data_list,
            'totalCount': total_count,
            'pageNo': pageNo
        }
    return None

def fetch_data_from_api(url, api_type, params):
    """
    실제 API 호출을 시도하고 오류를 처리하는 함수 (목업 사용)
    """
    print(f"[{api_type.upper()}] API 요청 중... (pageNo: {params.get('pageNo')})")
    
    # ----------------------------------------------------------------------
    # 실제 API 호출 로직 (목업 데이터를 반환하도록 임시 구현)
    try:
        # response = requests.get(url, params=params) 
        # response.raise_for_status() # HTTP 오류 발생 시 예외 발생
        # data = response.json()
        
        # 목업 데이터 사용
        data = get_mock_response(api_type, params.get('pageNo'))

        if data is None:
            return None, 0

        # API 내부 오류 체크 (예시: 서울시 API 응답 구조에 따라 다를 수 있음)
        if data.get('header', {}).get('resultCode') != '00':
            print(f"API 응답 오류: {data.get('header', {}).get('resultMsg')}")
            return None, 0
            
        return data, data.get('totalCount', 0)
    
    except requests.exceptions.RequestException as e:
        print(f"HTTP 요청 실패: {e}")
        return None, 0
    except json.JSONDecodeError:
        print("API 응답 JSON 디코딩 실패")
        return None, 0
    except Exception as e:
        print(f"알 수 없는 오류 발생: {e}")
        return None, 0
    # ----------------------------------------------------------------------

# --- 데이터 수집 및 CSV 저장 함수 ---

def collect_and_save_data(url, api_type, data_key, fieldnames, filename):
    """
    OpenAPI에서 데이터를 수집하고 CSV 파일로 저장하는 메인 로직
    """
    print(f"\n[{filename}] 데이터 수집 시작...")
    collected_data = []
    pageNo = 1
    totalCount = float('inf')  # 초기에는 무한대로 설정하여 첫 페이지 요청 보장
    
    try:
        while (pageNo - 1) * PAGE_SIZE < totalCount:
            
            # API 요청 파라미터 구성
            params = {
                'key': API_KEY,
                'pageNo': pageNo,
                'numOfRows': PAGE_SIZE,
                # 기타 필수 파라미터 (예: TYPE=json 등)
            }
            
            data, count = fetch_data_from_api(url, api_type, params)
            
            if data is None:
                # 오류 발생 또는 더 이상 데이터 없음
                break
                
            totalCount = count # 전체 건수 업데이트
            
            # 데이터 추출
            records = data.get(data_key, [])
            if not records:
                break

            # 필요한 필드만 추출하여 저장
            for record in records:
                # 위경도 좌표계 데이터를 사용하기 위해 'y'와 'x' 필드를 추출
                extracted = {key: record.get(key) for key in fieldnames}
                collected_data.append(extracted)
            
            pageNo += 1
            # 실제 API 호출 시에는 요청 간 지연 시간을 두는 것이 좋습니다.
            # time.sleep(0.1) 

        # --- CSV 파일 저장 ---
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader() # 제목줄 추가
            writer.writerows(collected_data)
            print(f"✅ {filename}에 총 {len(collected_data)}건의 데이터 저장 완료.")
            
    except Exception as e:
        # 문제 요구사항: 오류 발생 시 "구간(또는 지점) 데이터를 가져오는데 실패했습니다." 메시지 출력
        data_type_kr = "지점" if api_type == 'node' else "구간"
        print(f"❌ {data_type_kr} 데이터를 가져오는데 실패했습니다. (내부 오류: {e})")
        return

def fetch_and_save_nodes():
    """지점(노드) 데이터를 수집하고 spot_data.csv에 저장"""
    fieldnames = ['nodeId', 'y', 'x']
    collect_and_save_data(NODE_URL, 'node', 'nodeList', fieldnames, 'spot_data.csv')

def fetch_and_save_links():
    """구간(링크) 데이터를 수집하고 section_data.csv에 저장"""
    fieldnames = ['linkID', 'stnodeID', 'ednodeID', 'mapDist']
    collect_and_save_data(LINK_URL, 'link', 'linkList', fieldnames, 'section_data.csv')


if __name__ == "__main__":
    # 데이터 확보 시작
    print("--- 서울시 도로망 MVP 데이터 확보 스크립트 실행 ---")
    
    # 지점 데이터 수집 및 저장
    fetch_and_save_nodes()
    
    # 구간 데이터 수집 및 저장
    fetch_and_save_links()
    
    print("\n--- 스크립트 실행 완료 ---")
