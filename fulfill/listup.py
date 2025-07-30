import os
import sys
from sshtunnel import SSHTunnelForwarder
import pymysql
from dotenv import load_dotenv
import pandas as pd

def resource_path(relative_path):
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    # 개발 환경: listup.py가 있는 폴더 기준
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), relative_path)

# 실행 파일 환경에서 .env 파일 찾기
if getattr(sys, 'frozen', False):
    # PyInstaller로 빌드된 경우
    application_path = os.path.dirname(sys.executable)
    # PyInstaller 임시 디렉토리도 확인
    if hasattr(sys, '_MEIPASS'):
        temp_path = sys._MEIPASS
    else:
        temp_path = application_path
else:
    # 일반 Python 스크립트인 경우
    application_path = os.path.dirname(os.path.abspath(__file__))
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

# 환경변수에서 설정값 가져오기
SSH_HOST = os.getenv("SSH_HOST")
SSH_PORT = int(os.getenv("SSH_PORT", 22))
SSH_USER = os.getenv("SSH_USER")
SSH_KEY_PATH = os.getenv("SSH_KEY_PATH")  # .env에는 test.pem

# 반드시 resource_path로 변환
SSH_KEY_PATH = resource_path(SSH_KEY_PATH) if SSH_KEY_PATH else None

# 디버깅: 실제 경로와 파일 존재 여부 출력
print(f"실제 SSH_KEY_PATH: {SSH_KEY_PATH}")
print(f"실제 파일 존재 여부: {os.path.exists(SSH_KEY_PATH) if SSH_KEY_PATH else 'NO PATH'}")
if hasattr(sys, '_MEIPASS'):
    print(f"_MEIPASS 임시폴더: {sys._MEIPASS}")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_ORDER_SERVICE = os.getenv("DB_ORDER_SERVICE")

# SSH 키 파일 경로 처리
if SSH_KEY_PATH:
    if getattr(sys, 'frozen', False):
        # PyInstaller로 빌드된 경우 - 절대 경로 사용
        if not os.path.isabs(SSH_KEY_PATH):
            # 상대 경로인 경우 절대 경로로 변환
            SSH_KEY_PATH = os.path.abspath(SSH_KEY_PATH)
        print(f"PyInstaller mode - SSH Key path: {SSH_KEY_PATH}")
    else:
        # 로컬 개발 환경 - 상대 경로 허용
        if not os.path.isabs(SSH_KEY_PATH):
            SSH_KEY_PATH = os.path.join(application_path, SSH_KEY_PATH)
        print(f"Local mode - SSH Key path: {SSH_KEY_PATH}")
    
    print(f"SSH Key exists: {os.path.exists(SSH_KEY_PATH)}")
else:
    print("WARNING: SSH_KEY_PATH not set!")

if not SSH_HOST or not SSH_USER or not DB_HOST or not DB_USER:
    print("ERROR: Required environment variables are missing!")
    print("Please check your .env file.")
    input("Press Enter to exit...")
    exit(1)

  
def get_delivery_data(delivery_date: str):
    query = "CALL order_service.get_delivery_list(%s)"

    with SSHTunnelForwarder(
        (SSH_HOST, SSH_PORT),
        ssh_username=SSH_USER,
        ssh_pkey=SSH_KEY_PATH,
        remote_bind_address=(DB_HOST, DB_PORT)
    ) as tunnel:
        conn = pymysql.connect(
            host='127.0.0.1',
            port=tunnel.local_bind_port,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_ORDER_SERVICE,
            charset='utf8mb4'
        )

        try:
            # pandas로 직접 읽기 (커서 재사용 문제 방지)
            df = pd.read_sql(query, conn, params=[delivery_date])            
            # 만약 여전히 문제가 있다면 cursor로 직접 변환
            if df.empty and len(df.columns) > 0:
                with conn.cursor() as cursor:
                    cursor.execute(query, [delivery_date])
                    results = cursor.fetchall()
                    if results:
                        df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])            
        finally:
            conn.close()

        return df


def get_unique_filename(base_filename):
    """
    파일이 이미 존재하면 _1, _2, ... 를 붙여서 고유한 파일명을 반환
    """
    if not os.path.exists(base_filename):
        return base_filename
    name, ext = os.path.splitext(base_filename)
    counter = 1
    while True:
        new_filename = f"{name}_{counter}{ext}"
        if not os.path.exists(new_filename):
            return new_filename
        counter += 1


def safe_exit(msg=None):
    if msg:
        print(msg)
    input("Press Enter to exit...")
    exit(1)


def main():
    try:
        print("=== 배송 데이터 조회 및 Excel 저장 ===")
        # 배송일자 입력 받기
        while True:
            try:
                delivery_date = input("배송일자를 입력하세요 (YYYY-MM-DD 형식): ").strip()
                # 날짜 형식 검증
                if len(delivery_date) == 10 and delivery_date[4] == '-' and delivery_date[7] == '-':
                    year, month, day = delivery_date.split('-')
                    if year.isdigit() and month.isdigit() and day.isdigit():
                        if 1 <= int(month) <= 12 and 1 <= int(day) <= 31:
                            break
                        else:
                            print("올바른 월(1-12)과 일(1-31)을 입력해주세요.")
                    else:
                        print("숫자만 입력해주세요.")
                else:
                    print("YYYY-MM-DD 형식으로 입력해주세요. (예: 2025-07-31)")
            except KeyboardInterrupt:
                print("\n프로그램을 종료합니다.")
                input("Press Enter to exit...")
                return
            except Exception as e:
                print(f"입력 오류: {e}")

        print(f"\n배송일자: {delivery_date}")
        print("데이터를 조회 중입니다...")

        # 데이터 조회
        df = get_delivery_data(delivery_date)
        if df.empty:
            print(f"{delivery_date} 배송 데이터가 없습니다.")
            input("Press Enter to exit...")
            return

        # Excel 파일로 저장 (중복 방지)
        excel_filename = f"delivery_data_{delivery_date.replace('-', '')}.xlsx"
        excel_filename = get_unique_filename(excel_filename)
        df.to_excel(excel_filename, index=False, sheet_name='배송데이터')

        print(f"\n=== 완료 ===")
        print(f"Excel 파일이 저장되었습니다: {excel_filename}")
        print(f"저장된 데이터 개수: {len(df)}행")
        print(f"저장 위치: {os.path.abspath(excel_filename)}")
        input("Press Enter to exit...")

    except Exception as e:
        import traceback
        print("오류가 발생했습니다:")
        traceback.print_exc()
        input("Press Enter to exit...")
        exit(1)

if __name__ == "__main__":
    main()