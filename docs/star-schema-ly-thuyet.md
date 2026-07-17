# Star schema — lý thuyết thiết kế

Lý thuyết mô hình chiều (dimensional modeling) theo Kimball, **giải thích bằng chính dữ liệu
của repo này**. Tài liệu này trả lời "vì sao thiết kế như vậy"; còn [Star schema](star-schema.md)
mô tả "thiết kế hiện tại ra sao".

## Mục lục

- [Quy trình 4 bước của Kimball](#quy-trình-4-bước-của-kimball)
- [Bus matrix — bản đồ toàn cảnh](#bus-matrix--bản-đồ-toàn-cảnh)
- [Bốn loại bảng fact](#bốn-loại-bảng-fact)
- [Ba loại số đo](#ba-loại-số-đo)
- [Năm loại dimension](#năm-loại-dimension)
- [SCD — xử lý thay đổi của dimension](#scd--xử-lý-thay-đổi-của-dimension)
- [Star hay snowflake](#star-hay-snowflake)
- [Quan hệ nhiều-nhiều và bridge table](#quan-hệ-nhiều-nhiều-và-bridge-table)
- [Khoá: natural hay surrogate](#khoá-natural-hay-surrogate)
- [Những sai lầm kinh điển](#những-sai-lầm-kinh-điển)

---

## Quy trình 4 bước của Kimball

Kimball đưa ra đúng 4 bước, **theo thứ tự này, không đảo được**:

### Bước 1 — Chọn quy trình nghiệp vụ (business process)

Không phải chọn "bảng" hay "report", mà chọn **một hoạt động có thật của doanh nghiệp**:
khách đặt hàng, kho xuất hàng, khách trả hàng.

**Vì sao là quy trình chứ không phải phòng ban?** Vì nếu thiết kế theo phòng ban ("mart cho
Marketing", "mart cho Kế toán"), hai bên sẽ định nghĩa "doanh thu" khác nhau và không bao giờ
khớp số. Thiết kế theo quy trình thì cả hai cùng đọc một fact.

Repo này chọn: **khách đặt hàng** → `fact_order_items`.

### Bước 2 — Khai báo hạt (grain)

**Một dòng trong fact nghĩa là gì?** Viết ra thành một câu tiếng Việt hoàn chỉnh trước khi
chọn cột.

> "Một dòng = một dòng hàng trong một đơn."

**Vì sao đây là bước quan trọng nhất?** Vì mọi thứ sau đó phụ thuộc vào nó. Chọn hạt sai thì
không sửa được bằng cách thêm cột — phải làm lại từ đầu.

**Nguyên tắc: luôn chọn hạt MỊN NHẤT có thể.** Đừng thiết kế `fact_revenue_daily` (hạt: ngày)
chỉ vì hôm nay bạn chỉ cần báo cáo theo ngày. Mai sếp hỏi "theo sản phẩm" là chịu. Hạt mịn
luôn tổng hợp lên được; hạt thô không tách nhỏ ra được.

Repo này chọn hạt "dòng hàng" — mịn nhất có thể — nên `gold_revenue_daily` (theo ngày) vẫn
dựng được từ nó, còn ngược lại thì không.

### Bước 3 — Xác định dimension

Hỏi: **"Người ta mô tả sự kiện này bằng những cách nào?"** Mỗi câu trả lời là một dimension.

Với "khách đặt một dòng hàng": *khi nào* (dim_date), *ai mua* (dim_customer), *mua gì*
(dim_product), *có khuyến mãi gì* (dim_promotion).

Mẹo: dimension thường là các từ đứng sau "theo" trong câu hỏi business — "doanh thu **theo
vùng**, **theo tháng**, **theo category**".

### Bước 4 — Xác định số đo (facts)

Hỏi: **"Ta đo cái gì?"** Số đo phải đúng hạt đã khai báo ở bước 2.

`quantity`, `gross_amount`, `discount_amount`, `net_amount` — tất cả đều là số đo *của một
dòng hàng*, đúng hạt.

Nếu bạn thấy mình muốn thêm "tổng tiền của cả đơn" vào đây — dừng lại. Đó là số đo ở hạt
*đơn*, không phải hạt *dòng hàng*. Nhét vào thì `sum()` sẽ cộng lặp.

---

## Bus matrix — bản đồ toàn cảnh

Bus matrix là công cụ lập kế hoạch: **hàng = quy trình nghiệp vụ, cột = dimension**. Đánh dấu
X vào ô nào dùng được.

Bus matrix cho dữ liệu của repo này:

| Quy trình nghiệp vụ | dim_date | dim_customer | dim_product | dim_promotion | Trạng thái |
|---|:---:|:---:|:---:|:---:|---|
| **Đặt hàng** (`fact_order_items`) | X | X | X | X | ✅ đã làm |
| **Thanh toán** (`payments`) | X | X | | | ⬜ |
| **Giao hàng** (`shipments`) | X | X | | | ⬜ |
| **Trả hàng** (`returns`) | X | X | X | | ⬜ |
| **Đánh giá** (`reviews`) | X | X | X | | ⬜ |
| **Tồn kho** (`inventory`) | X | | X | | ⬜ |
| **Lưu lượng web** (`web_traffic`) | X | | | | ⬜ |

**Đọc bus matrix theo cột** là chỗ giá trị nhất: `dim_date` xuất hiện ở **cả 7 quy trình**,
`dim_product` ở 4. Nghĩa là chúng phải là **conformed dimension** — dùng chung một bản duy
nhất, không phải mỗi fact tự dựng một bản riêng.

**Vì sao quan trọng?** Nếu `fact_order_items` và `fact_returns` mỗi bên tự định nghĩa
"category", sớm muộn hai định nghĩa lệch nhau, và câu hỏi "category nào doanh thu cao nhưng
tỷ lệ trả hàng cũng cao" trở nên **không trả lời được**. Dùng chung `dim_product` thì so sánh
chéo luôn hợp lệ.

Bus matrix cũng cho bạn thứ tự làm: quy trình nào dùng nhiều dimension có sẵn nhất thì làm
trước — chi phí thấp, giá trị cao.

---

## Bốn loại bảng fact

Đây là phần lý thuyết hữu ích nhất, vì chọn sai loại là thiết kế sai từ gốc.

### 1. Transaction fact — một dòng cho một sự kiện

Phổ biến nhất. Sự kiện xảy ra → thêm một dòng. Không bao giờ sửa dòng cũ.

- **Ví dụ trong repo:** `fact_order_items` (đã làm), `fact_returns`, `fact_reviews`
- **Hạt:** một giao dịch
- **Số đo:** additive
- **Đặc điểm:** thưa (chỉ có dòng khi có sự kiện), lớn nhanh theo thời gian

### 2. Periodic snapshot fact — chụp ảnh định kỳ

Đo trạng thái tại các mốc thời gian đều đặn, kể cả khi không có gì xảy ra.

- **Ví dụ trong repo:** `inventory` — tồn kho của mỗi sản phẩm vào cuối mỗi tháng
- **Hạt:** sản phẩm × tháng
- **Số đo:** **semi-additive** (xem mục dưới) — đây là đặc trưng của loại này
- **Đặc điểm:** dày đặc (mọi sản phẩm đều có dòng mỗi tháng, kể cả tồn = 0)

**Vì sao không dùng transaction fact cho tồn kho?** Vì câu hỏi "tồn kho tháng 3 là bao nhiêu"
sẽ phải cộng dồn mọi giao dịch nhập/xuất từ đầu lịch sử — chậm và dễ sai. Snapshot trả lời
ngay.

### 3. Accumulating snapshot fact — theo dõi vòng đời

Một dòng cho **một thực thể đi qua nhiều mốc**, và dòng đó **được cập nhật** khi mốc mới xảy ra.

- **Ví dụ trong repo:** `fact_orders` = gộp `orders` + `shipments`
- **Hạt:** một đơn hàng
- **Cột đặc trưng:** nhiều cột ngày trên cùng một dòng

```
order_date -> ship_date -> delivery_date
```

Nhờ đó đo được **khoảng thời gian giữa các mốc** ngay trên một dòng:

```sql
date_diff('day', order_date, ship_date)     as ngay_cho_gui,
date_diff('day', ship_date, delivery_date)  as ngay_van_chuyen,
date_diff('day', order_date, delivery_date) as tong_thoi_gian
```

**Đây là loại duy nhất mà dòng cũ được UPDATE**, khác hẳn transaction fact. Đơn `created` hôm
nay, mai `shipped` → cập nhật `ship_date` vào chính dòng đó.

Với repo này, dữ liệu đã đứng yên (dừng 2022-12-31) nên `fact_orders` chỉ cần build một lần.

### 4. Factless fact — sự kiện không có số đo

Bảng fact **không có cột số đo nào**, chỉ toàn khoá ngoại. Nghe vô lý nhưng rất hữu ích.

Dùng để trả lời hai loại câu hỏi:

- **"Chuyện gì đã xảy ra"** khi bản thân việc xảy ra đã là thông tin (sinh viên có mặt lớp học).
- **"Chuyện gì ĐÃ KHÔNG xảy ra"** — loại câu hỏi khó nhất, và là lý do chính để dùng.

**Ví dụ trong repo:** một bảng `factless_promo_coverage` với hạt "khuyến mãi × ngày nó đang
chạy" (join `promotions` với `dim_date` theo khoảng `start_date`–`end_date`). Không có số đo
nào. Nhưng nó trả lời được:

- "Tháng 7/2015 có bao nhiêu khuyến mãi đang chạy cùng lúc?"
- **"Khuyến mãi nào đang chạy mà KHÔNG ai dùng?"** — câu này không fact nào khác trả lời được,
  vì `fact_order_items` chỉ có dòng khi *có người dùng* khuyến mãi.

Nguyên tắc chung: **dữ liệu chỉ ghi lại cái đã xảy ra; muốn hỏi về cái không xảy ra, bạn cần
một bảng liệt kê mọi khả năng.** Đây cũng chính là lý do `dim_date` phải tự sinh đầy đủ.

---

## Ba loại số đo

Phân biệt được ba loại này là biết ngay số đo nào `sum()` được, số nào không.

### Additive — cộng được theo MỌI dimension

`quantity`, `gross_amount`, `discount_amount`, `net_amount`.

Cộng theo ngày ra doanh thu ngày, theo vùng ra doanh thu vùng, theo cả hai vẫn đúng. **Đây là
loại tốt nhất và là lý do star schema hoạt động** — mọi report chỉ là `sum()` + `group by`.

### Semi-additive — cộng được theo MỘT SỐ dimension, trừ thời gian

`inventory.stock_on_hand` là ví dụ chuẩn.

| Cộng theo | Có nghĩa không? |
|---|---|
| Sản phẩm (tồn của A + tồn của B trong tháng 3) | ✅ tổng tồn kho tháng 3 |
| **Thời gian** (tồn tháng 1 + tồn tháng 2) | ❌ **vô nghĩa** |

Tồn kho tháng 1 là 100, tháng 2 vẫn là 100 — không có nghĩa là "200". Đó là **cùng 100 cái
hàng** được đếm hai lần.

**Cách xử lý đúng:** theo thời gian thì dùng `avg()`, hoặc lấy giá trị ở mốc cuối cùng
(`last_value`), không bao giờ `sum()`.

Số dư tài khoản ngân hàng, số nhân viên, tồn kho — mọi thứ "tại một thời điểm" đều semi-additive.

### Non-additive — không cộng được theo bất kỳ chiều nào

Mọi **tỷ lệ** và **giá đơn vị**:

- `unit_price` — cộng giá của hai dòng hàng ra số vô nghĩa
- `inventory.fill_rate`, `sell_through_rate` — cộng hai tỷ lệ % ra số vô nghĩa
- `web_traffic.bounce_rate`

**Quy tắc vàng cho tỷ lệ:** đừng lưu tỷ lệ rồi cộng nó. **Lưu tử số và mẫu số (đều additive),
rồi chia SAU KHI đã cộng.**

```sql
-- SAI: cộng tỷ lệ
select avg(fill_rate) from fact_inventory;

-- ĐÚNG: cộng hai thành phần rồi mới chia
select sum(units_delivered) * 1.0 / sum(units_demanded) from fact_inventory;
```

Vì sao? Trung bình của các tỷ lệ ≠ tỷ lệ của các tổng. Ngày A bán 1/1 (100%), ngày B bán
1/99 (1%). Trung bình tỷ lệ = 50,5%. Tỷ lệ thật = 2/100 = 2%. Lệch 25 lần.

---

## Năm loại dimension

### 1. Conformed dimension — dùng chung nhiều fact

`dim_date`, `dim_product` dùng cho cả `fact_order_items` lẫn `fact_returns` tương lai.

**Đây là ý tưởng giá trị nhất của Kimball.** Nó đảm bảo "category" trong báo cáo doanh thu và
trong báo cáo trả hàng là **cùng một định nghĩa**, nên so sánh chéo được. Một mớ report rời rạc
không bao giờ đảm bảo nổi điều đó.

### 2. Degenerate dimension — khoá không có dimension

`order_id` nằm thẳng trên fact, không có bảng `dim_order`.

**Vì sao?** Sau khi `order_date` sang `dim_date`, `customer_id` sang `dim_customer`, bảng
`orders` **không còn thuộc tính nào** để mô tả. Một dimension chỉ có mỗi khoá là dimension
rỗng — vô dụng. Nên khoá "thoái hoá" (degenerate) xuống ở lại fact.

Vẫn dùng được: `count(distinct order_id)` để đếm đơn.

### 3. Junk dimension — gom các cờ lặt vặt

Khi có nhiều cột ít giá trị (`order_status` 6 giá trị, `payment_method` 5, `device_type` 3,
`order_source` 4), làm 4 dimension tí hon thì thừa, để cả 4 trên fact thì fact phình.

**Junk dimension** gom chúng thành một bảng nhỏ liệt kê các **tổ hợp thực tế có xảy ra**, fact
chỉ giữ một khoá.

Repo này **chọn không làm** — để thẳng 4 cột trên fact, vì ở quy mô này đơn giản hơn và không
mất gì. Đây là ví dụ lý thuyết nói một đằng, thực tế chọn một nẻo **có lý do**.

### 4. Role-playing dimension — một dimension đóng nhiều vai

Cùng `dim_date` nhưng đóng ba vai khác nhau trong `fact_orders`:

```sql
join dim_date d_order    on f.order_date_key    = d_order.date_key
join dim_date d_ship     on f.ship_date_key     = d_ship.date_key
join dim_date d_delivery on f.delivery_date_key = d_delivery.date_key
```

Một bảng vật lý, ba alias. Nhờ đó hỏi được "đơn đặt cuối tuần có giao chậm hơn không" —
`d_order.is_weekend` và `d_delivery.is_weekend` là hai thứ khác nhau.

### 5. Degenerate vs Junk vs Role-playing — phân biệt nhanh

| Loại | Đặc điểm | Ví dụ |
|---|---|---|
| Degenerate | Khoá ở lại fact, không có bảng dim | `order_id` |
| Junk | Gom nhiều cột ít giá trị vào 1 dim | `order_status` + `payment_method` + ... |
| Role-playing | 1 bảng dim, nhiều vai trong 1 fact | `dim_date` cho order/ship/delivery |

---

## SCD — xử lý thay đổi của dimension

**Slowly Changing Dimension**: khách chuyển nhà, sản phẩm đổi category. Xử lý sao?

| Type | Cách làm | Hậu quả |
|---|---|---|
| **Type 0** | Không bao giờ đổi | Dùng cho thứ bất biến: ngày sinh, `signup_date` |
| **Type 1** | **Ghi đè**, mất lịch sử | Báo cáo lịch sử **đổi theo** giá trị mới |
| **Type 2** | **Thêm dòng mới**, giữ dòng cũ + `valid_from`/`valid_to` | Báo cáo lịch sử **giữ nguyên** — chuẩn nhất |
| **Type 3** | Thêm cột `previous_value` | Chỉ nhớ được 1 lần đổi |
| **Type 4** | Tách bảng lịch sử riêng | Dim chính gọn, lịch sử ở bảng phụ |
| **Type 6** | Kết hợp 1+2+3 | Phức tạp, ít dùng |

### Repo này dùng Type 1, và vì sao

`dim_customer` là SCD Type 1 — chỉ có trạng thái hiện tại.

**Không phải vì Type 1 tốt hơn, mà vì Type 2 BẤT KHẢ THI ở đây.** Nguồn chỉ có 1 dòng mỗi
khách, **không có cột `valid_from`/`valid_to` nào**. Lịch sử chưa bao giờ được ghi lại →
không thể dựng lại cái đã mất.

**Muốn Type 2 thì phải bắt đầu từ hôm nay:** dùng `dbt snapshot` chụp trạng thái dimension
định kỳ, và từ đó về sau có lịch sử. Quá khứ thì không cứu được.

### Hệ quả thật của Type 1 trong repo này

Khách chuyển từ vùng `east` sang `west` → mọi đơn cũ của họ **bị gán lại vùng `west`**. Báo cáo
"doanh thu 2015 theo vùng" chạy hôm nay ra số khác với chạy năm ngoái, dù dữ liệu 2015 không
đổi dòng nào.

Điều thú vị: **nguồn `orders.zip` ĐÃ giữ địa chỉ lịch sử của từng đơn**. Nên bạn có hai lựa chọn:

| Câu hỏi | Dùng |
|---|---|
| "Đơn này được giao tới vùng nào" (lịch sử đúng) | `orders.zip` → `geography` |
| "Khách này hiện ở vùng nào" (trạng thái hiện tại) | `dim_customer.region` |

Hôm nay hai cái cho cùng kết quả (100% trùng khớp), nhưng chúng trả lời **hai câu hỏi khác
nhau** và sẽ tách ra khi có khách chuyển nhà.

---

## Star hay snowflake

**Snowflake** = dimension được chuẩn hoá tiếp thành nhiều tầng. Nguồn của repo chính là
snowflake: `customers → geography`.

**Star** = làm phẳng hết vào dimension.

| | Snowflake | Star |
|---|---|---|
| Lưu trữ | Gọn (40k zip lưu 1 lần) | Lặp (122k khách) |
| Số join cho "doanh thu theo vùng" | 2 | **1** |
| Dễ hiểu với người dùng | Khó | **Dễ** |
| Cập nhật khi zip đổi tên | 1 chỗ | Nhiều dòng |

**Repo này chọn star** — `dim_customer` mang luôn `city`/`region`/`district`.

**Vì sao đánh đổi này gần như luôn đúng với dimension?** Vì dimension nhỏ, đọc nhiều, ghi ít.
122k dòng lặp vài cột text tốn vài MB — không đáng gì so với việc mọi query bớt được một join.
Nếu là fact 714k dòng thì tính khác, nhưng dimension thì cứ làm phẳng.

**Khi nào snowflake hợp lý?** Khi dimension cực lớn (hàng chục triệu dòng) và thuộc tính lặp
chiếm dung lượng thật sự đáng kể. Hiếm gặp.

---

## Quan hệ nhiều-nhiều và bridge table

Star schema chuẩn giả định fact–dimension là **nhiều-một**: một dòng hàng thuộc *một* sản phẩm.

Nhưng dữ liệu thật hay có nhiều-nhiều. **Ví dụ trong repo:** một dòng hàng có thể mang **2
khuyến mãi** (`promo_id` và `promo_id_2`).

Ba cách xử lý:

| Cách | Làm sao | Khi nào dùng |
|---|---|---|
| **Bỏ qua** | Chỉ giữ cái đầu tiên | Tỷ lệ nhiều-nhiều rất nhỏ |
| **Cột riêng** | `promo_key`, `promo_key_2` | Số lượng tối đa cố định và nhỏ |
| **Bridge table** | Bảng trung gian `line × promo` | Đúng nhất, nhưng phức tạp |

**Repo này chọn cách 1** — `promo_id_2` chỉ giữ làm degenerate, phân tích theo `promo_key` chỉ
tính khuyến mãi thứ nhất.

**Vì sao chấp nhận được?** Vì chỉ **206/714.669 dòng (0,03%)** có khuyến mãi thứ hai. Dựng
bridge table cho 0,03% là đổi rất nhiều phức tạp lấy rất ít chính xác.

**Bridge table trông thế nào** (nếu sau này cần):

```
bridge_line_promo: (order_id, product_id, promo_id)
```

Fact join bridge, bridge join dim_promotion. **Cảnh báo:** join qua bridge sẽ **nhân dòng
fact** — dòng có 2 promo thành 2 dòng. Nên `sum(net_amount)` sau khi join bridge sẽ đếm tiền
2 lần. Đây là lý do bridge table nguy hiểm và chỉ nên dùng khi thật sự cần.

---

## Khoá: natural hay surrogate

**Natural key** = khoá nghiệp vụ của nguồn (`customer_id = 58578`).
**Surrogate key** = khoá số tự sinh, vô nghĩa với nghiệp vụ (`customer_key = 84732`).

Kimball khuyên **luôn dùng surrogate key**. Lý do:

1. **Bắt buộc cho SCD Type 2** — một khách có nhiều dòng, `customer_id` hết unique.
2. **Khoá nguồn có thể bẩn** — trùng, đổi kiểu, tái sử dụng sau khi xoá.
3. **Cô lập mart khỏi nguồn** — nguồn đổi hệ thống, mart không cần đổi.
4. **Xử lý được thành viên "không xác định"** — khoá `-1` cho dữ liệu thiếu.

**Repo này dùng natural key.** Vì đã kiểm chứng: **không có vấn đề nào trong bốn**.

- Khoá nguồn sạch tuyệt đối (121.930/121.930 unique; 0 mồ côi trên 14 khoá ngoại)
- Không có SCD Type 2 (và không thể có — xem mục SCD)
- Nguồn là file CSV tĩnh, không đổi
- Thành viên "không xác định" đã giải quyết bằng `'NO_PROMO'` (khoá văn bản, đọc được)

Thêm surrogate key lúc này chỉ tạo một tầng dịch khoá phải bảo trì, và làm mọi lần debug khó
hơn — nhìn `customer_key = 84732` không biết là ai, còn `customer_id = 58578` thì tra thẳng
về nguồn được.

**Khi nào bắt buộc phải đổi:** ngày bạn làm SCD Type 2. Lúc đó `customer_id` hết unique và
natural key sụp đổ.

> **Bài học rộng hơn:** quy tắc trong sách đi kèm bối cảnh của nó. Hiểu quy tắc **giải quyết
> vấn đề gì**, rồi kiểm tra xem bạn có vấn đề đó không. Áp dụng mù quáng thì trả giá phức tạp
> mà không nhận lại gì.

---

## Những sai lầm kinh điển

| Sai lầm | Vì sao sai | Bằng chứng trong repo |
|---|---|---|
| **Chọn hạt quá thô** | Không tách nhỏ ra được sau này | Chọn "dòng hàng" nên vẫn dựng được `gold_revenue_daily` theo ngày |
| **`count(*)` sau khi join fact** | Đếm dòng hàng thành số đơn | Phải `count(distinct order_id)` |
| **Khoá ngoại NULL trong fact** | `join` âm thầm vứt dòng | Để `promo_id` NULL → inner join mất **438.353 dòng (61,3%)** |
| **`sum()` số đo non-additive** | Ra số vô nghĩa | `sum(unit_price)`, `avg(fill_rate)` |
| **Lẫn giá dimension với giá fact** | Ra số sai âm thầm | `products.price` chỉ khớp `unit_price` ở **76/714.669 dòng (0,01%)** |
| **Trộn nhiều hạt trong 1 fact** | `sum()` cộng lặp | Nhét "tổng tiền đơn" vào fact hạt dòng hàng |
| **Không có test hạt** | Join nhân dòng là lỗi im lặng | `tests/assert_fact_order_items_grain.sql` |
| **Dimension thiếu thành viên** | Không hỏi được "cái gì KHÔNG xảy ra" | `dim_date` tự sinh đủ 4.018 ngày |

**Sai lầm nguy hiểm nhất không nằm trong bảng: thiết kế trước khi nhìn dữ liệu.** Ba quyết
định lớn nhất của repo này — `NO_PROMO`, `current_list_price`, natural key — đều đến từ việc
chạy query kiểm chứng *trước*, không từ việc đọc sách.
