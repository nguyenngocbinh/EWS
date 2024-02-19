# EWS

## Tổng Quan
Vận hành hàng tháng mô hình Bscore

## Cài Đặt
Hướng dẫn cách cài đặt và thiết lập dự án.

## Sử Dụng
Giải thích cách sử dụng dự án, bao gồm bất kỳ lệnh hoặc cấu hình nào cần thiết.

### Bước 1: Import Dữ Liệu
1. Các bảng IFRS9 kéo từ DB2 sang (Code day bang DB2 sang SQL.ipynb)
2. Các bảng cl020, visa028, mis035, sử dụng code .R

### Bước 2: Chạy Test Cases
1. Tại cửa số command promt chạy lệnh:
   `python main_unittest.py`
2. Kiểm tra lỗi được list ra trong quá trình chạy test case.

### Bước 3: EWS_DATA_PREPARATION.sql
1. Sử dụng `EWS_DATA_PREPARATION.sql` để tạo shortlist cho mô hình Bscore.
2. Với mô hình retail thì sẽ tạo ra shortlist lưu trong bảng: `EWS..ews_portfolio_shortlist_store`
3. Với mô hình non-retail thì vừa tạo ra shortlist và score, grade lưu trong bảng: `ews..ews_khdn_score_store`

### Bước 4: Tính PSI - KS cho từng biến.ipynb
1. Tính toán PSI, KS để kiểm tra độ drift của biến đầu vào

### Bước 5: ews_predict_datamart.R
1. Kéo dữ liệu từ bảng `EWS_PORTFOLIO_SHORTLIST_STORE` trên DB
2. Chạy mô hình Lightgbm để predict score, grade cho mô hình retail
3. Đẩy kết quả dự báo lên DB: bảng `ews..ews_khcn_portfolio_score_store`, `ews..ews_khcn_customer_score_store`

## Cấu trúc thư mục
```
EWS
├─ db.py
├─ import_table
│  ├─ Code day bang DB2 sang SQL.ipynb
│  ├─ daybang_cl020.R
│  ├─ daybang_mis035b.R
│  └─ daybang_visa028.R
├─ main_unittest.py
├─ RUN_MONTHLY
│  ├─ EWS_DATA_PREPARATION.sql
│  ├─ ews_predict_datamart.R
│  └─ Tính PSI - KS cho từng biến.ipynb
└─ testcases
   ├─ ifrs9_ctr_cl_cl020.py
   ├─ ifrs9_ctr_od.py
   ├─ ifrs9_cust_info.py
   ├─ ifrs9_dep_amt_txn.py
   ├─ mis076.py
   ├─ r18.py
   ├─ visa028.py
   └─ visa031_sv.py

```
