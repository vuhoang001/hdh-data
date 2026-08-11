# Mô hình dữ liệu — kiến thức nền

Đây là **trang gốc** của phần mô hình hoá dữ liệu trong repo này. Nó không dạy lại lý thuyết
suông — mỗi khái niệm đều được **giải thích bằng chính dữ liệu thật của repo** (13 bảng bronze,
714.669 dòng hàng), và mỗi con số đều có cách tự kiểm chứng lại.

> **Điểm khác biệt:** biết "SCD Type 2 là gì" không đồng nghĩa với biết "khi nào dùng nó". SQL
> chạy đúng, test xanh, nhưng vẫn có thể mô hình sai từ gốc. Phần này tập trung vào **quyết định
> thiết kế** — thứ còn đúng qua nhiều thế hệ công cụ — chứ không phải cú pháp của một công cụ cụ thể.

---

## Nền tảng (Reference)

Năm khái niệm cốt lõi. Đọc theo thứ tự này nếu mới bắt đầu.

| # | Chủ đề | Trả lời câu hỏi | Mức | Trạng thái |
| --- | --- | --- | --- | --- |
| 1 | [Hạt (grain)](star-schema.md#hạt--quyết-định-quan-trọng-nhất) | "Một dòng nghĩa là **gì**" — quyết định quan trọng nhất | Cơ bản | ✅ Đã kiểm chứng |
| 2 | [Fact và Dimension](star-schema.md#star-schema-là-gì-và-giải-quyết-vấn-đề-gì) | Hai loại bảng; "fact là động từ, dimension là trạng từ" | Cơ bản | ✅ Đã kiểm chứng |
| 3 | [Natural key hay Surrogate key](star-schema-ly-thuyet.md#khoá-natural-hay-surrogate) | Vì sao Kimball khuyên surrogate — và vì sao repo này *không* dùng | Trung cấp | ✅ Đã kiểm chứng |
| 4 | [Quy trình 4 bước Kimball](star-schema-ly-thuyet.md#quy-trình-4-bước-của-kimball) | Từ nghiệp vụ tới bảng — đúng thứ tự, không đảo được | Trung cấp | ✅ Đã kiểm chứng |
| 5 | [Star / Snowflake](star-schema-ly-thuyet.md#star-hay-snowflake) | Ba cách bố trí bảng; khi nào làm phẳng, khi nào chuẩn hoá | Trung cấp | ✅ Đã kiểm chứng |

---

## Kỹ thuật áp dụng (Skills)

Xây trên nền tảng. Đây là chỗ lý thuyết gặp dữ liệu thật và đôi khi **chọn ngược sách** — có lý do.

| # | Chủ đề | Trả lời câu hỏi | Mức | Trạng thái |
| --- | --- | --- | --- | --- |
| 1 | [Lực lượng quan hệ (cardinality)](mo-hinh-du-lieu.md#lực-lượng-quan-hệ-cardinality) | Khi nào `join`, khi nào `left join`, khi nào `count(*)` cho số sai | Trung cấp | ✅ Đã kiểm chứng |
| 2 | [Bốn loại bảng fact](star-schema-ly-thuyet.md#bốn-loại-bảng-fact) | Transaction / snapshot / accumulating / factless — chọn sai là sai từ gốc | Trung cấp | ✅ Đã kiểm chứng |
| 3 | [Ba loại số đo](star-schema-ly-thuyet.md#ba-loại-số-đo) | Số nào `sum()` được, số nào không (additive / semi / non) | Trung cấp | ✅ Đã kiểm chứng |
| 4 | [SCD — xử lý thay đổi](star-schema-ly-thuyet.md#scd--xử-lý-thay-đổi-của-dimension) | Type 0–6; vì sao Type 2 **bất khả thi** với nguồn này | Trung cấp | ✅ Đã kiểm chứng |
| 5 | [Năm loại dimension](star-schema-ly-thuyet.md#năm-loại-dimension) | Conformed, degenerate, junk, role-playing — phân biệt nhanh | Nâng cao | ✅ Đã kiểm chứng |
| 6 | [Bus matrix](star-schema-ly-thuyet.md#bus-matrix--bản-đồ-toàn-cảnh) | Bản đồ toàn cảnh: quy trình nào × dimension nào, làm gì trước | Nâng cao | ✅ Đã kiểm chứng |
| 7 | [Nhiều-nhiều và Bridge table](star-schema-ly-thuyet.md#quan-hệ-nhiều-nhiều-và-bridge-table) | Xử lý quan hệ nhiều-nhiều mà không đếm tiền 2 lần | Nâng cao | ✅ Đã kiểm chứng |

---

## Hướng dẫn thực hành (Tutorials)

| Chủ đề | Nội dung | Trạng thái |
| --- | --- | --- |
| [Thêm một bảng mới vào pipeline](them-bang-moi.md) | Đi hết chặng CSV → bronze → silver → gold, dùng `order_items` làm ví dụ | ✅ Đã kiểm chứng |
| [Star schema — thiết kế hiện tại](star-schema.md) | `models/marts/` có gì, mỗi quyết định vì sao chọn thế | ✅ Đã kiểm chứng |

---

## Tra nhanh (Cheatsheets)

| Chủ đề | Nội dung |
| --- | --- |
| [Công thức join](mo-hinh-du-lieu.md#công-thức-join) | Doanh thu theo ngày / category / vùng, tỷ lệ giao hàng, tỷ lệ trả hàng |
| [Ba lỗi join hay gặp nhất](mo-hinh-du-lieu.md#ba-lỗi-join-hay-gặp-nhất) | `count(*)` sau join · `join` thay `left join` · join sai hạt |
| [Những sai lầm kinh điển](star-schema-ly-thuyet.md#những-sai-lầm-kinh-điển) | 8 sai lầm thiết kế, kèm bằng chứng trong repo |
| [Tự kiểm chứng lại](mo-hinh-du-lieu.md#tự-kiểm-chứng-lại) | Query để tự đo lại mọi con số trong tài liệu |

---

## Bài học thực chiến (Case studies)

Mỗi kỹ thuật ở trên đều có ít nhất một tình huống thật trong dữ liệu này:

| Tình huống | Kỹ thuật liên quan | Ở đâu |
| --- | --- | --- |
| Query sai hạt "phát hiện" 3 vi phạm ảo | Hạt (grain) | [Một bài học về chính tài liệu này](mo-hinh-du-lieu.md#một-bài-học-về-chính-tài-liệu-này) |
| 564 đơn đã giao mà không có shipment | Kiểm chứng chéo | [Bất thường đã phát hiện](mo-hinh-du-lieu.md#bất-thường-đã-phát-hiện) |
| `promo_id_2` dễ bị gắn cờ lỗi oan | Rule chất lượng | [promo_id_2 — không phải lỗi](mo-hinh-du-lieu.md#promo_id_2--không-phải-lỗi-nhưng-dễ-hiểu-nhầm) |
| `sales_daily` lệch 18% với `order_items` | Hai nguồn độc lập | [Hai bảng không nối được với ai](mo-hinh-du-lieu.md#hai-bảng-không-nối-được-với-ai) |
| Khách chuyển nhà → báo cáo lịch sử đổi | SCD Type 1 | [Hệ quả thật của Type 1](star-schema-ly-thuyet.md#hệ-quả-thật-của-type-1-trong-repo-này) |

---

## Lộ trình học

```text
SQL (join, group by)
        ↓
Hạt (grain) ← bắt đầu ở đây
        ↓
Fact và Dimension
        ↓
Natural / Surrogate key
        ↓
Lực lượng quan hệ (cardinality) ← chỗ dễ sai nhất
        ↓
Quy trình 4 bước Kimball
        ↓
Star / Snowflake
        ↓
Thực hành: Thêm bảng mới → build fact/dim
```

**Đường ngắn nhất đủ dùng:** Hạt → Fact/Dimension → Cardinality → Công thức join.

---

## Điều hướng liên quan

- [Mô hình dữ liệu — 13 bảng bronze](mo-hinh-du-lieu.md) — quan hệ và cách join nguồn
- [Star schema — thiết kế](star-schema.md) — mart hiện tại ra sao
- [Star schema — lý thuyết](star-schema-ly-thuyet.md) — vì sao thiết kế thế
- [Thêm bảng mới](them-bang-moi.md) — cách viết job/model
- [README gốc của repo](../README.md) — kiến trúc tổng thể, cách chạy

**Cập nhật lần cuối:** 2026-08-02
