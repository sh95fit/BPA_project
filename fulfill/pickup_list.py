import os
import sys
from sshtunnel import SSHTunnelForwarder
import pymysql
from dotenv import load_dotenv
import pandas as pd

# PyInstaller 환경에서 .env 파일 찾기
if getattr(sys, 'frozen', False):
    # PyInstaller로 빌드된 경우
    application_path = os.path.dirname(sys.executable)
    # PyInstaller 임시 디렉토리 확인
    if hasattr(sys, '_MEIPASS'):
        temp_path = sys._MEIPASS
    else:
        temp_path = application_path
    
    # .env 파일 경로 설정 (여러 위치 확인)
    env_paths = [
        os.path.join(application_path, '.env'),
        os.path.join(temp_path, '.env'),
        '.env'  # 현재 작업 디렉토리
    ]
    
    env_loaded = False
    for env_path in env_paths:
        print(f"Looking for .env at: {env_path}")
        print(f"File exists: {os.path.exists(env_path)}")
        
        if os.path.exists(env_path):
            load_dotenv(env_path)
            env_loaded = True
            print(f"Successfully loaded .env from: {env_path}")
            break
    
    if not env_loaded:
        print("WARNING: .env file not found in any location!")
        # 기본 .env 로드 시도
        load_dotenv()
else:
    # 일반 Python 스크립트인 경우
    load_dotenv()

# 환경변수에서 설정값 가져오기
SSH_HOST = os.getenv("SSH_HOST")
SSH_PORT = int(os.getenv("SSH_PORT", 22))
SSH_USER = os.getenv("SSH_USER")
SSH_KEY_PATH = os.getenv("SSH_KEY_PATH")
SSH_PASSWORD = os.getenv("SSH_PASSWORD")  # SSH 비밀번호 추가

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_ORDER_SERVICE = os.getenv("DB_ORDER_SERVICE")

# SSH 인증 방식 결정
ssh_auth_method = None
if SSH_KEY_PATH:
    # SSH 키 파일 경로 처리
    if getattr(sys, 'frozen', False):
        # PyInstaller로 빌드된 경우 - 절대 경로 사용
        if not os.path.isabs(SSH_KEY_PATH):
            # 상대 경로인 경우 절대 경로로 변환
            SSH_KEY_PATH = os.path.abspath(SSH_KEY_PATH)
        print(f"PyInstaller mode - SSH Key path: {SSH_KEY_PATH}")
    else:
        # 로컬 개발 환경 - 상대 경로 허용
        if not os.path.isabs(SSH_KEY_PATH):
            SSH_KEY_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), SSH_KEY_PATH)
        print(f"Local mode - SSH Key path: {SSH_KEY_PATH}")
    
    if os.path.exists(SSH_KEY_PATH):
        ssh_auth_method = "key"
        print(f"SSH Key authentication will be used: {SSH_KEY_PATH}")
    else:
        print(f"WARNING: SSH key file not found: {SSH_KEY_PATH}")
        SSH_KEY_PATH = None

if not ssh_auth_method and SSH_PASSWORD:
    ssh_auth_method = "password"
    print("SSH Password authentication will be used")

if not ssh_auth_method:
    print("ERROR: No SSH authentication method available!")
    print("Please set either SSH_KEY_PATH or SSH_PASSWORD in your .env file")
    input("Press Enter to exit...")
    exit(1)

# 필수 환경변수 검증
missing_vars = []
if not SSH_HOST:
    missing_vars.append("SSH_HOST")
if not SSH_USER:
    missing_vars.append("SSH_USER")
if not DB_HOST:
    missing_vars.append("DB_HOST")
if not DB_USER:
    missing_vars.append("DB_USER")
if not DB_PASSWORD:
    missing_vars.append("DB_PASSWORD")
if not DB_ORDER_SERVICE:
    missing_vars.append("DB_ORDER_SERVICE")

if missing_vars:
    print("ERROR: Required environment variables are missing!")
    print(f"Missing variables: {', '.join(missing_vars)}")
    print("Please check your .env file.")
    input("Press Enter to exit...")
    exit(1)

# 환경 변수 디버깅 정보 출력
print("\n=== Environment Variables Debug Info ===")
print(f"SSH_HOST: {'***' if SSH_HOST else 'NOT SET'}")
print(f"SSH_PORT: {SSH_PORT}")
print(f"SSH_USER: {'***' if SSH_USER else 'NOT SET'}")
print(f"SSH_KEY_PATH: {'***' if SSH_KEY_PATH else 'NOT SET'}")
print(f"SSH_PASSWORD: {'***' if SSH_PASSWORD else 'NOT SET'}")
print(f"DB_HOST: {'***' if DB_HOST else 'NOT SET'}")
print(f"DB_PORT: {DB_PORT}")
print(f"DB_USER: {'***' if DB_USER else 'NOT SET'}")
print(f"DB_PASSWORD: {'***' if DB_PASSWORD else 'NOT SET'}")
print(f"DB_ORDER_SERVICE: {'***' if DB_ORDER_SERVICE else 'NOT SET'}")
print(f"SSH Auth Method: {ssh_auth_method}")
print("=" * 40)


def get_pickup_data_by_keyword(address_keyword: str):
    """주소 키워드로 픽업 데이터 조회"""
    query = "CALL order_service.get_pickup_list(%s)"
    
    # SSH 터널 설정 (인증 방식에 따라 다르게 설정)
    if ssh_auth_method == "key":
        tunnel = SSHTunnelForwarder(
            (SSH_HOST, SSH_PORT),
            ssh_username=SSH_USER,
            ssh_pkey=SSH_KEY_PATH,
            remote_bind_address=(DB_HOST, DB_PORT)
        )
    else:  # password
        tunnel = SSHTunnelForwarder(
            (SSH_HOST, SSH_PORT),
            ssh_username=SSH_USER,
            ssh_password=SSH_PASSWORD,
            remote_bind_address=(DB_HOST, DB_PORT)
        )

    with tunnel:
        conn = pymysql.connect(
            host='127.0.0.1',
            port=tunnel.local_bind_port,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_ORDER_SERVICE,
            charset='utf8mb4'
        )

        try:
            df = pd.read_sql(query, conn, params=[address_keyword])
            
            # 만약 여전히 문제가 있다면 cursor로 직접 변환
            if df.empty and len(df.columns) > 0:
                with conn.cursor() as cursor:
                    cursor.execute(query, [address_keyword])
                    results = cursor.fetchall()
                    if results:
                        df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])
            
        finally:
            conn.close()

        return df


def get_delivery_date_input():
    """배송일자 입력 받기"""
    print("=== 배송일자 입력 ===")
    
    while True:
        try:
            delivery_date = input("배송일자를 입력하세요 (YYYY-MM-DD 형식): ").strip()
            
            # 날짜 형식 검증
            if len(delivery_date) == 10 and delivery_date[4] == '-' and delivery_date[7] == '-':
                year, month, day = delivery_date.split('-')
                if year.isdigit() and month.isdigit() and day.isdigit():
                    if 1 <= int(month) <= 12 and 1 <= int(day) <= 31:
                        return delivery_date
                    else:
                        print("올바른 월(1-12)과 일(1-31)을 입력해주세요.")
                else:
                    print("숫자만 입력해주세요.")
            else:
                print("YYYY-MM-DD 형식으로 입력해주세요. (예: 2025-07-31)")
        except KeyboardInterrupt:
            print("\n프로그램을 종료합니다.")
            return None
        except Exception as e:
            print(f"입력 오류: {e}")


def process_address_keywords_from_input():
    """사용자 입력으로 주소 키워드 목록 처리"""
    print("\n=== 주소 키워드 입력 ===")
    print("Excel에서 복사한 주소 키워드들을 붙여넣으세요.")
    print("(여러 줄로 입력 가능, 빈 줄 입력 시 종료)")
    print("-" * 50)
    
    keywords = []
    while True:
        try:
            line = input().strip()
            if not line:  # 빈 줄 입력 시 종료
                break
            if line:  # 빈 문자열이 아닌 경우만 추가
                keywords.append(line)
        except KeyboardInterrupt:
            print("\n입력을 중단합니다.")
            break
        except EOFError:
            break
    
    return keywords


def main():
    """메인 실행 함수"""
    print("=== 주소 키워드로 픽업 데이터 조회 ===")
    
    # 1. 배송일자 입력 받기
    delivery_date = get_delivery_date_input()
    if delivery_date is None:
        return
    
    print(f"배송일자: {delivery_date}")
    
    # 2. 주소 키워드 목록 입력 받기
    address_keywords = process_address_keywords_from_input()
    
    if not address_keywords:
        print("입력된 키워드가 없습니다.")
        return
    
    print(f"\n입력된 키워드 개수: {len(address_keywords)}")
    print("키워드 목록:")
    for i, keyword in enumerate(address_keywords, 1):
        print(f"  {i}. {keyword}")
    
    print("\n데이터를 조회 중입니다...")
    
    # 모든 결과를 저장할 리스트
    all_results = []
    
    # 각 키워드별로 데이터 조회
    for i, keyword in enumerate(address_keywords, 1):
        print(f"처리 중... ({i}/{len(address_keywords)}) {keyword}")
        
        try:
            df = get_pickup_data_by_keyword(keyword)
            
            if not df.empty:
                # 키워드 정보 추가
                df['search_keyword'] = keyword
                all_results.append(df)
                print(f"  → {len(df)}건 조회됨")
            else:
                print(f"  → 데이터 없음")
                
        except Exception as e:
            print(f"  → 오류: {e}")
    
    if not all_results:
        print("\n조회된 데이터가 없습니다.")
        return
    
    # 모든 결과를 하나의 DataFrame으로 합치기
    final_df = pd.concat(all_results, ignore_index=True)
    
    print(f"\n=== 조회 완료 ===")
    print(f"총 조회된 데이터: {len(final_df)}건")
    print(f"키워드별 조회 결과:")
    
    # 키워드별 통계
    keyword_stats = final_df.groupby('search_keyword').size()
    for keyword, count in keyword_stats.items():
        print(f"  {keyword}: {count}건")
    
    # Excel 파일로 저장 (배송일자 포함, 중복 시 번호 추가)
    base_filename = f"pickup_data_{delivery_date.replace('-', '')}"
    excel_filename = f"{base_filename}.xlsx"
    
    # 파일이 이미 존재하는 경우 번호 추가
    counter = 1
    while os.path.exists(excel_filename):
        excel_filename = f"{base_filename}_{counter}.xlsx"
        counter += 1
    
    final_df.to_excel(excel_filename, index=False, sheet_name='픽업데이터')
    
    print(f"\n=== 파일 저장 완료 ===")
    print(f"Excel 파일: {excel_filename}")
    print(f"저장 위치: {os.path.abspath(excel_filename)}")
    
    # 결과 미리보기
    # print(f"\n=== 결과 미리보기 (처음 5건) ===")
    # print(final_df.head().to_string(index=False))


if __name__ == "__main__":
    main()
