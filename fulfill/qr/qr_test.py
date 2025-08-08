import qrcode

url = "https://www.lunchlab.me/"

# QR 코드 객체 생성 (옵션 설정 가능)
qr = qrcode.QRCode(
    version=1,  # 1~40, 숫자가 클수록 용량과 크기 증가 (1은 가장 작음)
    error_correction=qrcode.constants.ERROR_CORRECT_L,  # 오류정정 수준 (L:7%)
    box_size=10,  # 각 점 크기 (픽셀 단위)
    border=4  # 테두리 두께 (점 개수 단위)
)

# 데이터 추가
qr.add_data(url)
qr.make(fit=True)

# 이미지 생성 (흑백)
img = qr.make_image(fill_color="black", back_color="white")


# 이미지 파일로 저장
img.save("qr/delivery_qr.png")

print("QR 코드 이미지가 'delivery_qr.png' 파일로 저장되었습니다.")