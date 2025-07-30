import os
import sys
from sshtunnel import SSHTunnelForwarder
import pymysql
from dotenv import load_dotenv
import pandas as pd

def resource_path(relative_path):
    """PyInstaller 호환 파일 경로"""
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), relative_path)

def load_env():
    """다양한 위치에서 .env 파일 로드"""
    paths = [
        os.path.join(sys._MEIPASS, '.env') if hasattr(sys, '_MEIPASS') else None,
        os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else None,
        os.path.dirname(os.path.abspath(__file__)),
        os.getcwd()
    ]
    for path in filter(None, paths):
        env_path = os.path.join(path, '.env')
        if os.path.exists(env_path):
            load_dotenv(env_path)
            print(f"[INFO] .env 로드 완료: {env_path}")
            return True
    print("[WARNING] .env 파일을 찾을 수 없습니다.")
    return False

def prepare_ssh_key_path(path):
    if not path:
        return None
    abs_path = resource_path(path)
    if not os.path.exists(abs_path):
        print(f"[ERROR] SSH 키 파일이 존재하지 않음: {abs_path}")
        return None
    return abs_path

def get_delivery_data(delivery_date: str):
    query = "CALL order_service.get_delivery_list(%s)"

    with SSHTunnelForwarder(
        (SSH_HOST, SSH_PORT),
        ssh_username=SSH_USER,
        ssh_pkey=SSH_KEY_PATH,
        remote_bind_address=(DB_HOST, DB_PORT)
    ) as tunnel:
        with pymysql.connect(
            host='127.0.0.1',
            port=tunnel.local_bind_port,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_ORDER_SERVICE,
            charset='utf8mb4'
        ) as conn:
            try:
                df = pd.read_sql(query, conn, params=[delivery_date])
                if df.empty and len(df.columns) > 0:
                    with conn.cursor() as cursor:
                        cursor.execute(query, [delivery_date])
                        rows = cursor.fetchall()
                        if rows:
                            df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])
            except Exception as e:
                print(f"[ERROR] 데이터 조회 중 오류: {e}")
                raise
    return df

def get_unique_filename(base):
    name, ext = os.path.splitext(base)
    counter = 1
    while os.path.exists(base):
        base = f"{name}_{counter}{ext}"
        counter += 1
    return base

def prompt_date():
    while True:
        try:
            delivery_date = input("배송일자 (YYYY-MM-DD): ").strip()
            if (
                len(delivery_date) == 10 and delivery_date[4] == '-' and delivery_date[7] == '-'
                and delivery_date.replace('-', '').isdigit()
            ):
                year, month, day = map(int, delivery_date.split('-'))
                if 1 <= month <= 12 and 1 <= day <= 31:
                    return delivery_date
            print("❌ 올바른 형식의 날짜를 입력하세요 (예: 2025-07-31)")
        except KeyboardInterrupt:
            print("\n프로그램을 종료합니다.")
            sys.exit(0)

def main():
    if not load_env():
        input("Press Enter to exit...")
        return

    global SSH_HOST, SSH_PORT, SSH_USER, SSH_KEY_PATH
    global DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_ORDER_SERVICE

    SSH_HOST = os.getenv("SSH_HOST")
    SSH_PORT = int(os.getenv("SSH_PORT", 22))
    SSH_USER = os.getenv("SSH_USER")
    SSH_KEY_PATH = prepare_ssh_key_path(os.getenv("SSH_KEY_PATH"))

    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = int(os.getenv("DB_PORT", 3306))
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_ORDER_SERVICE = os.getenv("DB_ORDER_SERVICE")

    if not all([SSH_HOST, SSH_USER, SSH_KEY_PATH, DB_HOST, DB_USER]):
        print("[ERROR] 필수 환경변수가 누락되었습니다.")
        input("Press Enter to exit...")
        return

    print("=== 배송 데이터 조회 ===")
    delivery_date = prompt_date()

    print(f"\n📦 배송일자: {delivery_date}")
    print("데이터 조회 중...")

    try:
        df = get_delivery_data(delivery_date)
    except Exception as e:
        print("❌ 데이터 조회 실패:", e)
        input("Press Enter to exit...")
        return

    if df.empty:
        print(f"해당 일자({delivery_date})에 대한 배송 데이터가 없습니다.")
        input("Press Enter to exit...")
        return

    filename = get_unique_filename(f"delivery_data_{delivery_date.replace('-', '')}.xlsx")
    df.to_excel(filename, index=False, sheet_name='배송데이터')

    print("\n✅ 저장 완료!")
    print(f"파일명: {filename}")
    print(f"행 수: {len(df)}")
    print(f"경로: {os.path.abspath(filename)}")
    input("Press Enter to exit...")

if __name__ == "__main__":
    main()
