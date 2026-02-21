


# I. 8 Bước Xây Dựng Kho Dữ Liệu — Hướng Dẫn Chi Tiết

**Mục 1 — Giới thiệu**
Viết văn mô tả bài toán: doanh nghiệp là gì, có bao nhiêu DB nguồn, mục tiêu xây DW để làm gì. Ngắn thôi, 1-2 trang.

**Mục 2 — Yêu cầu nghiệp vụ**
Diễn giải lại 9 câu query thành ngôn ngữ nghiệp vụ — người dùng cần biết gì, kịch bản phân tích là gì. Đây là phần bạn hỏi lúc nãy.

**Mục 3 — Đặc tả chức năng**
Đây là phần **nhiều người hay bỏ qua hoặc làm sơ sài nhất**. Cô hỏi: đầu vào của kho là gì, đầu ra là gì. Cụ thể:
- Đầu vào: 2 DB OLTP nguồn, schema nào, bảng nào, cột nào được lấy vào
- Đầu ra: 9 báo cáo OLAP tương ứng 9 câu query, mỗi báo cáo output trông như thế nào

**Mục 4 — Thiết kế kho dữ liệu**
Đây là phần kỹ thuật nặng nhất — bao gồm toàn bộ những gì mình đã bàn: ERD nguồn, IDB tích hợp, Star Schema (Fact + Dimension), hierarchy các chiều, ETL mapping.

**Mục 5 — Cài đặt các khối dữ liệu**
Chạy thực tế trên máy — DDL tạo bảng, script ETL load data, chụp màn hình chứng minh data đã vào DW. Đây là phần code nhiều nhất.

**Mục 6 — Báo cáo OLAP**
Viết 9 câu SQL chạy trên DW, chụp kết quả. Nếu dùng Superset/dashboard thì nhúng ảnh chart vào đây.

**Mục 7 — Kiểm tra tính đúng đắn**
Với mỗi câu OLAP, chạy query tương đương trên OLTP gốc rồi so sánh kết quả. Nếu khớp → ETL đúng.

**Mục 8 — Kết luận**
Tổng kết những gì đã làm được, chưa làm được, bài học rút ra.

---

## Map sang công việc thực tế của nhóm

```
Mục 1, 2, 3  →  Viết doc, không cần code
Mục 4        →  Thiết kế (dbdiagram.io) + viết doc
Mục 5        →  Code nặng: DDL + ETL Python + Docker
Mục 6        →  Code: 9 SQL queries + dashboard
Mục 7        →  Code: validation queries
Mục 8        →  Viết doc
```

# II. Chi tiết từng mục

## Mục 1 — Giới thiệu

**Mục tiêu:** Xây dựng một kho dữ liệu cho hệ thống xử lý đặt hàng của một doanh nghiệp bán lẻ có nhiều cửa hàng phân tán ở nhiều thành phố và bang. Kho dữ liệu tích hợp dữ liệu từ 2 nguồn OLTP riêng biệt, phục vụ phân tích trực tuyến (OLAP) với các thao tác cuộn lên, khoan xuống, chọn và chiếu.

**Phạm vi:** Hệ thống bao gồm toàn bộ quy trình từ tích hợp dữ liệu nguồn, thiết kế kho, ETL, đến sinh báo cáo OLAP đáp ứng 9 yêu cầu nghiệp vụ cụ thể.

---

## Mục 2 — Yêu cầu nghiệp vụ

Đây là phần quan trọng nhất để bạn hiểu trước khi thiết kế. Mình phân tích 9 câu query thành nhóm có ý nghĩa:

**Nhóm 1 — Phân tích cửa hàng và hàng tồn kho** (câu 1, 4, 7)

Người quản lý cần biết bức tranh tổng thể về hàng hóa đang nằm ở đâu trong hệ thống. Câu 1 hỏi toàn bộ cửa hàng đang bán gì. Câu 4 hỏi văn phòng đại diện nào đang có hàng tồn kho vượt ngưỡng — đây là báo cáo cảnh báo tồn kho cao. Câu 7 hỏi mức tồn kho của 1 mặt hàng cụ thể tại tất cả cửa hàng trong 1 thành phố — đây là báo cáo kiểm tra khả năng đáp ứng đơn hàng.

**Nhóm 2 — Phân tích đơn hàng** (câu 2, 5, 8)

Người quản lý cần trace được đơn hàng từ đầu đến cuối. Câu 2 hỏi danh sách đơn hàng theo khách hàng. Câu 5 và 8 đi sâu hơn — với mỗi đơn hàng, mặt hàng nào được đặt, cửa hàng nào có thể cung cấp, ở thành phố nào. Đây là nghiệp vụ cốt lõi của hệ thống: **đơn hàng được fulfil từ kho nào**.

**Nhóm 3 — Phân tích khách hàng** (câu 3, 6, 9)

Câu 6 đơn giản — khách hàng sống ở đâu. Câu 3 phức tạp hơn — cửa hàng nào bán hàng cho khách hàng đó, tức là trace ngược từ khách hàng ra cửa hàng. Câu 9 là phân tích phân khúc khách hàng — du lịch, bưu điện, hay cả hai.

---

**Kịch bản nghiệp vụ tổng thể** có thể diễn đạt như sau:

> Doanh nghiệp cần biết: kho hàng của mình đang ở trạng thái nào, đơn hàng đang được xử lý ra sao, và khách hàng của mình là ai — để tối ưu việc phân phối hàng từ đúng kho đến đúng khách.

---

## Mục 3 — Đặc tả chức năng

**Đầu vào — 2 nguồn OLTP:**

DB Văn phòng đại diện cung cấp thông tin về khách hàng — họ là ai, sống ở đâu, thuộc loại nào (du lịch hay bưu điện).

DB Bán hàng cung cấp thông tin về địa điểm (VP đại diện, cửa hàng), hàng hóa (mặt hàng, tồn kho), và giao dịch (đơn đặt hàng, chi tiết đơn hàng).
```
Container postgres_oltp
├── schema: vanphong_db    ← DB Văn phòng đại diện
│   ├── khachhang
│   ├── khachhang_dulich
│   └── khachhang_buudien
└── schema: banhang_db     ← DB Bán hàng
    ├── vanphongdaidien
    ├── cuahang
    ├── mathang
    ├── mathang_luutru
    ├── dondathang
    └── mathang_duocdat

Container postgres_dw      ← Data Warehouse
└── schema: dw
    ├── dim_*
    └── fact_*
```

**Đầu ra — 9 báo cáo OLAP:**

| Câu | Đầu ra |
|-----|--------|
| 1 | Bảng: CuaHang × MatHang — thành phố, bang, SĐT, mô tả, kích cỡ, trọng lượng, đơn giá |
| 2 | Bảng: KhachHang × DonHang — tên KH, mã đơn, ngày đặt |
| 3 | Bảng: CuaHang có bán hàng cho KH X — tên TP, SĐT |
| 4 | Bảng: VP đại diện có tồn kho MH Y > ngưỡng N — địa chỉ VP, tên TP, bang |
| 5 | Bảng: Đơn hàng × MatHang × CuaHang — mô tả MH, mã CH, tên TP |
| 6 | Bảng: KH X — tên TP, bang |
| 7 | Bảng: MatHang Y tại TP Z — mã CH, số lượng tồn |
| 8 | Bảng: Đơn hàng X — MH, số lượng đặt, KH, CH, TP |
| 9 | Bảng: 3 nhóm KH — du lịch only, bưu điện only, cả hai |

**Các thao tác OLAP được hỗ trợ:**

- Roll-up: từ cửa hàng → thành phố → bang
- Drill-down: từ bang → thành phố → cửa hàng cụ thể
- Slice: lọc theo 1 khách hàng, 1 mặt hàng, 1 thành phố
- Dice: kết hợp nhiều điều kiện, ví dụ mặt hàng X tại thành phố Y trong tháng Z

## Mục 4 — Thiết kế kho dữ liệu

### Bước 1 — Chuyển OLTP sang ERD + tích hợp (IDB)

Giống nhóm mẫu đã làm — vẽ ERD từng DB, sau đó tích hợp lại thành 1 sơ đồ duy nhất (IDB). Điểm cần xử lý:

- `KhachHangDuLich` và `KhachHangBuuDien` là subtype của `KhachHang` — khái quát hóa giao nhau (1 KH có thể thuộc cả 2)
- `DonDatHang.MaKH` → FK cross-schema sang `KhachHang`
- Cột `ThoiGian` ở nhiều bảng → đồng âm, cần đổi tên cho rõ nghĩa

IDB sau khi tích hợp:

```
VanPhongDaiDien (1) ──── (N) CuaHang
VanPhongDaiDien (1) ──── (N) KhachHang
KhachHang (1) ──── (N) DonDatHang
KhachHang (1) ──── (0,1) KhachHangDuLich
KhachHang (1) ──── (0,1) KhachHangBuuDien
CuaHang (N) ──── (M) MatHang  →  MatHangLuuTru
DonDatHang (N) ──── (M) MatHang  →  MatHangDuocDat
```

---

### Bước 2 — Xác định Fact và Dimension

Từ 9 câu query, nhóm bạn cần **2 Fact**:

**FACT_DonDatHang** — grain: 1 dòng = 1 mặt hàng trong 1 đơn hàng
Phục vụ câu 2, 3, 5, 8, 9

**FACT_TonKho** — grain: 1 dòng = tồn kho của 1 mặt hàng tại 1 cửa hàng tại 1 thời điểm
Phục vụ câu 1, 4, 7

Và **5 Dimension**:

```
DIM_ThoiGian     → chiều thời gian (ngày/tháng/quý/năm)
DIM_KhachHang    → tích hợp từ 3 bảng KH, xử lý subtype
DIM_MatHang      → thông tin mặt hàng
DIM_CuaHang      → cửa hàng + denormalize VP đại diện vào luôn
DIM_DiaDiem      → hierarchy: CuaHang → ThanhPho → Bang
                   (dùng cho roll-up/drill-down địa lý)
```


---

### Tư duy nền tảng

Hãy tưởng tượng bạn là người quản lý, bạn muốn **đo lường một sự kiện kinh doanh**. Mỗi khi có sự kiện xảy ra — khách đặt hàng, hàng được bán — bạn muốn ghi lại và phân tích nó.

Mọi sự kiện kinh doanh đều có 2 thành phần:

**Thứ nhất — Con số đo lường được** (Fact/Measure): bao nhiêu tiền, bao nhiêu cái, bao nhiêu kg. Đây là thứ bạn muốn SUM, AVG, COUNT.

**Thứ hai — Ngữ cảnh của con số đó** (Dimension): con số đó xảy ra *khi nào, ở đâu, với ai, cái gì*. Đây là thứ bạn dùng để GROUP BY, WHERE, FILTER.

---

### Ví dụ cực kỳ cụ thể

Sự kiện: *"Ngày 15/1/2024, khách hàng Nguyễn Văn A đặt 3 cái áo giá 200k tại cửa hàng Q1 TPHCM"*

```
Fact (đo lường):
├── so_luong_dat = 3
├── gia_dat      = 200,000
└── thanh_tien   = 600,000   ← cái này bạn muốn SUM

Dimension (ngữ cảnh):
├── Khi nào?  → 15/1/2024   → DIM_ThoiGian
├── Ai?       → Nguyễn Văn A → DIM_KhachHang
├── Cái gì?   → Áo           → DIM_MatHang
└── Ở đâu?   → CH Q1 TPHCM  → DIM_CuaHang
```

Bảng Fact chỉ lưu **khóa ngoại + con số**, không lưu tên hay mô tả:

```
FACT_DonDatHang:
| sk_tg | sk_kh | sk_mh | sk_ch | so_luong | gia_dat | thanh_tien |
|   42  |   7   |   15  |   3   |    3     | 200,000 |   600,000  |
```

Muốn biết tên khách hàng → JOIN sang DIM_KhachHang theo sk_kh = 7.
Muốn biết tháng mấy → JOIN sang DIM_ThoiGian theo sk_tg = 42.

---

### Tại sao cần 2 Fact cho bài này?

Vì có **2 loại sự kiện khác nhau hoàn toàn**:

**Sự kiện 1 — Đặt hàng** (transaction): xảy ra tại 1 thời điểm cụ thể, có khách hàng, có mặt hàng, có số tiền. Đây là sự kiện *động*.

**Sự kiện 2 — Tồn kho** (snapshot): không phải giao dịch mà là trạng thái tại 1 thời điểm — cửa hàng X đang có bao nhiêu mặt hàng Y. Đây là sự kiện *tĩnh*.

Gộp 2 loại này vào 1 Fact sẽ sai vì grain khác nhau — 1 bên là "mỗi lần đặt hàng", 1 bên là "mỗi lần kiểm kho". Khi JOIN chung sẽ gây ra kết quả sai lệch (gọi là fan trap).

---

### Grain là gì — quan trọng nhất

**Grain = mỗi dòng trong Fact table đại diện cho cái gì.**

Grain của FACT_DonDatHang = **1 mặt hàng trong 1 đơn hàng**. Nghĩa là nếu đơn hàng #001 có 3 mặt hàng khác nhau → 3 dòng trong Fact.

Grain của FACT_TonKho = **tồn kho của 1 mặt hàng tại 1 cửa hàng vào 1 ngày**. Nghĩa là mỗi ngày chạy snapshot → thêm N dòng mới vào Fact.

Grain phải xác định trước khi làm bất cứ thứ gì khác — grain sai thì toàn bộ schema sai.

---

### Surrogate Key là gì

Bạn thấy trong schema mình dùng `sk_khach_hang`, `sk_mat_hang`... thay vì dùng `ma_kh`, `ma_mh` trực tiếp. Lý do:

Trong OLTP, `ma_kh = 'KH001'` là natural key — do hệ thống nguồn tạo ra. Trong DW, bạn tạo thêm `sk_khach_hang = 1, 2, 3...` là số nguyên tự tăng — gọi là surrogate key.

Lý do cần surrogate key: khi dữ liệu thay đổi theo thời gian (SCD), bạn có thể có 2 version của cùng 1 khách hàng — version cũ và version mới. Surrogate key phân biệt được 2 version này, còn natural key thì không.

---

Tóm lại sơ đồ tư duy:

```
Sự kiện kinh doanh
├── Con số cần đo → FACT TABLE (measures + FK)
│   └── Grain = mỗi dòng là gì?
└── Ngữ cảnh của con số → DIMENSION TABLE
    ├── Khi nào?  → DIM_ThoiGian
    ├── Ai?       → DIM_KhachHang
    ├── Cái gì?   → DIM_MatHang
    └── Ở đâu?   → DIM_CuaHang / DIM_DiaDiem
```

---

### Bước 3 — Star Schema chi tiết

```
                    DIM_ThoiGian
                         |
DIM_KhachHang ──── FACT_DonDatHang ──── DIM_MatHang
                         |
                    DIM_CuaHang
                         |
                    DIM_DiaDiem


DIM_CuaHang ────  FACT_TonKho ──── DIM_MatHang
                       |
                  DIM_ThoiGian
```

Schema chi tiết:

```sql
-- FACT 1
FACT_DonDatHang (
    sk_thoi_gian,
    sk_khach_hang,
    sk_mat_hang,
    sk_cua_hang,
    ma_don,          -- degenerate dimension
    so_luong_dat,    -- measure
    gia_dat,         -- measure
    thanh_tien       -- measure (pre-computed)
)

-- FACT 2
FACT_TonKho (
    sk_thoi_gian,
    sk_mat_hang,
    sk_cua_hang,
    so_luong_ton     -- measure
)

-- DIMENSIONS
DIM_ThoiGian (sk_thoi_gian, ngay, thang, quy, nam, thu)

DIM_KhachHang (
    sk_khach_hang, ma_kh, ten_kh,
    loai_kh,          -- 'DuLich' | 'BuuDien' | 'CaHai'
    huong_dan_vien,   -- NULL nếu không phải du lịch
    dia_chi_buu_dien, -- NULL nếu không phải bưu điện
    ngay_dat_hang_dau_tien,
    ma_tp             -- FK sang DIM_DiaDiem
)

DIM_MatHang (
    sk_mat_hang, ma_mh, mo_ta,
    kich_co, trong_luong, gia
)

DIM_CuaHang (
    sk_cua_hang, ma_ch, so_dien_thoai,
    ma_tp, ten_tp, bang, dia_chi_vp  -- denormalize VP vào đây
)

DIM_DiaDiem (
    sk_dia_diem, ma_tp, ten_tp, bang, dia_chi_vp
)
```

---

### Bước 4 — Hierarchy cho OLAP operations

```
Địa lý:    CuaHang → ThanhPho → Bang
Thời gian: Ngay → Thang → Quy → Nam
KhachHang: MaKH → LoaiKH (DuLich / BuuDien / CaHai)
MatHang:   MaMH (flat, không có hierarchy)
```

Hierarchy địa lý này cho phép:
- **Roll-up**: tổng đơn hàng từng cửa hàng → gộp theo thành phố → gộp theo bang
- **Drill-down**: từ bang chọn vào thành phố → xem từng cửa hàng cụ thể
- **Slice**: chỉ xem 1 thành phố, 1 loại KH
- **Dice**: KH du lịch + TP Hà Nội + tháng 1/2024

## Mục 5 — Cài đặt các khối dữ liệu

Mục này yêu cầu **tự động bằng máy tính** — tức là không click tay, phải có script chạy được. Gồm 3 phần:

**Phần A — DDL tạo bảng**

Viết SQL tạo toàn bộ bảng trong DW — 5 Dimension + 2 Fact. Chạy một lần là xong.

**Phần B — ETL Pipeline**

Đây là phần phức tạp nhất, gồm 3 bước Extract → Transform → Load:

*Extract:* Copy raw data từ 2 schema OLTP vào staging area. Không transform gì ở bước này.

*Transform:* Xử lý logic phức tạp:
- Merge 3 bảng KhachHang thành DIM_KhachHang, xác định LoaiKH
- Join CuaHang + VanPhongDaiDien → DIM_CuaHang
- Generate DIM_ThoiGian từ ngày min đến ngày max
- SCD Type 2: kiểm tra nếu dữ liệu thay đổi thì expire record cũ, insert record mới
- Generate surrogate key

*Load:* Insert vào DW theo thứ tự — Dimension trước, Fact sau (vì Fact cần FK từ Dimension).

**Phần C — Chứng minh data đã vào**

Chạy query đếm số dòng từng bảng, chụp màn hình dán vào báo cáo.

---

## Mục 6 — Báo cáo OLAP

Phần này viết 9 câu SQL chạy trên DW, mỗi câu kèm kết quả. Mình map từng câu:

**Câu 1** — JOIN FACT_TonKho + DIM_CuaHang + DIM_MatHang
```sql
SELECT ch.ten_tp, ch.bang, ch.so_dien_thoai,
       mh.mo_ta, mh.kich_co, mh.trong_luong, mh.gia
FROM fact_ton_kho f
JOIN dim_cua_hang ch ON f.sk_cua_hang = ch.sk_cua_hang
JOIN dim_mat_hang mh ON f.sk_mat_hang = mh.sk_mat_hang
WHERE f.so_luong_ton > 0
```

**Câu 2** — JOIN FACT_DonDatHang + DIM_KhachHang + DIM_ThoiGian
```sql
SELECT kh.ten_kh, f.ma_don, tg.ngay
FROM fact_don_dat_hang f
JOIN dim_khach_hang kh ON f.sk_khach_hang = kh.sk_khach_hang
JOIN dim_thoi_gian tg ON f.sk_thoi_gian = tg.sk_thoi_gian
```

**Câu 7** — Roll-up tồn kho trong 1 thành phố (Slice + Dice)
```sql
SELECT ch.ma_ch, mh.mo_ta, SUM(f.so_luong_ton) as tong_ton
FROM fact_ton_kho f
JOIN dim_cua_hang ch ON f.sk_cua_hang = ch.sk_cua_hang
JOIN dim_mat_hang mh ON f.sk_mat_hang = mh.sk_mat_hang
WHERE mh.ma_mh = :ma_mh_input
  AND ch.ten_tp = :ten_tp_input
GROUP BY ch.ma_ch, mh.mo_ta
```

**Câu 9** — UNION/INTERSECT phân loại khách hàng
```sql
-- Du lịch only
SELECT ma_kh, ten_kh, 'Du lich' as loai
FROM dim_khach_hang WHERE loai_kh = 'DuLich'
UNION ALL
-- Bưu điện only
SELECT ma_kh, ten_kh, 'Buu dien' as loai
FROM dim_khach_hang WHERE loai_kh = 'BuuDien'
UNION ALL
-- Cả hai
SELECT ma_kh, ten_kh, 'Ca hai' as loai
FROM dim_khach_hang WHERE loai_kh = 'CaHai'
```

Ngoài 9 câu query, phần này cũng cần **minh họa các OLAP operation** bằng SQL cụ thể — ví dụ roll-up dùng `GROUP BY ROLLUP(bang, ten_tp, ma_ch)`, drill-down dùng WHERE filter dần từng cấp.

---

Bạn muốn đi tiếp Mục 7 — Kiểm tra tính đúng đắn, hay muốn dừng lại làm rõ phần nào?