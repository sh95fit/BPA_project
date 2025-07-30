import os
from sshtunnel import SSHTunnelForwarder
import pymysql
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

# 환경변수에서 설정값 가져오기
SSH_HOST = os.getenv("SSH_HOST")
SSH_PORT = int(os.getenv("SSH_PORT", 22))
SSH_USER = os.getenv("SSH_USER")
SSH_KEY_PATH = os.getenv("SSH_KEY_PATH")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_ORDER_SERVICE = os.getenv("DB_ORDER_SERVICE")

  
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


def main():
    """메인 실행 함수"""
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
            return
        except Exception as e:
            print(f"입력 오류: {e}")
    
    print(f"\n배송일자: {delivery_date}")
    print("데이터를 조회 중입니다...")
    
    try:
        # 데이터 조회
        df = get_delivery_data(delivery_date)
        
        if df.empty:
            print(f"{delivery_date} 배송 데이터가 없습니다.")
            return
        
        # Excel 파일로 저장
        excel_filename = f"delivery_data_{delivery_date.replace('-', '')}.xlsx"
        df.to_excel(excel_filename, index=False, sheet_name='배송데이터')
        
        print(f"\n=== 완료 ===")
        print(f"Excel 파일이 저장되었습니다: {excel_filename}")
        print(f"저장된 데이터 개수: {len(df)}행")
        print(f"저장 위치: {os.path.abspath(excel_filename)}")
        
    except Exception as e:
        print(f"오류가 발생했습니다: {e}")

if __name__ == "__main__":
    main()