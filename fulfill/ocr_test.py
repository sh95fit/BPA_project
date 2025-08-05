import pytesseract
import numpy as np
from PIL import Image
import cv2

# 이미지 개선
# filename = r"C:/Users/USER/OneDrive/Desktop/Company/업무내용/배송관련이슈/listup/fulfill/ocr_sample.png"
filename = r"fulfill\ocr_sample.png"
image = cv2.imread(filename)
# gray_image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
# blurred_image = cv2.GaussianBlur(gray_image, (5, 5), 0)
# equalized_image = cv2.equalizeHist(blurred_image)
# resized_image = cv2.resize(equalized_image, None, fx=2, fy=2, interpolation=cv2.INTER_CUBIC)

# # 이진화 - Otsu 임계값 자동설정
# _, binary = cv2.threshold(equalized_image, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)

# # (선택) 샤프닝
# kernel = np.array([[0,-1,0],[-1,5,-1],[0,-1,0]])
# sharpened = cv2.filter2D(binary, -1, kernel)

# # (선택) 크기 확대
# final_img = cv2.resize(sharpened, None, fx=2, fy=2, interpolation=cv2.INTER_CUBIC)



# image = Image.open(filename)
# custom_config = r"--psm 6 --oem 3 -c tessedit_char_whitelist=0123456789"
custom_config = r"--psm 6 --oem 3"
# text = pytesseract.image_to_string(blurred_image, lang="kor+eng")
text = pytesseract.image_to_string(image, lang="kor+eng", config=custom_config)

with open("./ocr_sample.txt", "w", encoding="utf-8") as f:
    f.write(text)
    print(text)
