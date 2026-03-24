### Note về thiết kế Data Warehouse và Cube

Dim_DiaDiem load từ VanPhongDaiDien, phục vụ cho 
Dim_CuaHang và Dim_KhachHang. Có 2 hướng xử lý:

Hướng 1 — Snowflake (giữ Dim_DiaDiem riêng):
- Dim_KhachHang có sk_diaDiem → Dim_DiaDiem
- Dim_CuaHang có sk_diaDiem → Dim_DiaDiem
- ETL đơn giản, không cần denormalize
- Khi tạo cube phải JOIN qua 2 bảng 
  (Fact → Dim_KhachHang/CuaHang → Dim_DiaDiem)
- Khi tạo cube phải JOIN qua 2 bảng 
  (Fact → Dim_KhachHang/CuaHang → Dim_DiaDiem) => phức tạp, 

Hướng 2 — Star Schema thuần túy (denormalize):
- Copy tenThanhPho, bang, diaChiVP vào Dim_KhachHang và Dim_CuaHang trực tiếp
- Bỏ hoàn toàn Dim_DiaDiem
- ETL phức tạp hơn (phải JOIN khi load Dim)
- Cube đơn giản, không cần JOIN thêm bảng trung gian 

Hướng 3 - Star Schema: 
- Vẫn giữ Dim_DiaDiem riêng, nhưng đưa sk_DiaDiem vào các Fact để JOIN trực tiếp, không cần JOIN qua Dim_KhachHang/CuaHang nữa
- ETL phức tạp hơn (phải JOIN khi load Fact)
- Cube đơn giản, không cần JOIN thêm bảng trung gian 