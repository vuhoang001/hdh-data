"""
Đọc ingestion/config/sources.yml — phần cấu hình mà CHỈ phía Spark cần.

Ranh giới giữa hai file cấu hình, cố ý không chồng lấn:

    ingestion/bronze_specs/bronze_<bảng>.sql   LOGIC — schema, chuẩn hoá, luật chất
                                                 lượng, cột dẫn xuất. dbt và Spark
                                                 cùng đọc file này.

    ingestion/config/sources.yml                 HẠ TẦNG INGEST — nguồn lấy ở đâu, ghi
                                                 Iceberg partition kiểu gì, log thêm số
                                                 liệu nào. dbt không cần và không đọc.

Trước đây `partition_by` ở đây chỉ là chú thích cho người đọc, còn giá trị thật nằm rải
trong 13 hàm partition_columns(). Giờ nó là nguồn duy nhất — chuỗi ở đây được phân tích
thành hàm biến đổi Iceberg thật.
"""
import re
from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Optional, Tuple

import yaml

# pyspark CỐ Ý không import ở cấp module: module này phải nạp được ở nơi không có Spark
# (CI lint, pytest). Chỉ partition_columns() mới cần, và nó import lúc gọi.

# Hàm partition Iceberg nhận ĐÚNG một cột: years(col), months(col), days(col), hours(col)
_TIME_TRANSFORMS = ("years", "months", "days", "hours")
# Hàm nhận một tham số width rồi tới cột: bucket(16, order_id).
# Iceberg còn có truncate() nhưng PySpark không expose nó qua DataFrame API, nên muốn dùng
# phải khai bằng DDL `CREATE TABLE ... PARTITIONED BY (truncate(...))`.
_WIDTH_TRANSFORMS = ("bucket",)

_CALL = re.compile(r"^\s*(\w+)\s*\(\s*([^)]*?)\s*\)\s*$")


class SourceSpecError(ValueError):
    """sources.yml khai sai — dừng ngay thay vì ingest ra bảng partition nhầm."""


@dataclass(frozen=True)
class SourceSpec:
    """Cấu hình ingest của một bảng bronze.

    table         : tên bảng trong namespace bronze
    type          : loại nguồn, phải khớp một reader đã đăng ký ở common/io.py.
                    ĐÂY LÀ CHỖ ĐỔI KHI LÊN PRODUCTION — đổi sang 'postgres' là bảng đó
                    lấy từ DB thay vì file, và model SQL không phải sửa dòng nào.
    partition_by  : chuỗi khai chiến lược partition Iceberg, hoặc None nếu để một file
    extra_metrics : {nhãn: điều kiện SQL} để log thêm số liệu riêng của bảng
    """

    table: str
    type: str
    partition_by: Optional[str] = None
    extra_metrics: Mapping[str, str] = field(default_factory=dict)

    def parse_partition(self) -> Optional[Tuple[str, List[str]]]:
        """Phân tích chuỗi partition_by thành (tên hàm, tham số) và kiểm tính hợp lệ.

        Tách khỏi partition_columns() để test kiểm được khai báo mà KHÔNG cần Spark —
        khai sai partition thì bảng Iceberg ra sai layout, và đó là thứ chỉ phát hiện
        được sau khi đã ghi xong vài trăm nghìn dòng.
        """
        if not self.partition_by:
            return None

        call = _CALL.match(self.partition_by)
        if not call:
            raise SourceSpecError(
                f"{self.table}: partition_by='{self.partition_by}' không đúng dạng "
                f"hàm(tham số). Ví dụ: months(order_date), bucket(16, order_id), "
                f"hoặc bỏ trống để không partition."
            )

        name, args = call.group(1), [a.strip() for a in call.group(2).split(",")]

        if name in _TIME_TRANSFORMS:
            if len(args) != 1:
                raise SourceSpecError(
                    f"{self.table}: {name}() nhận đúng 1 cột, đang có {args}."
                )
        elif name in _WIDTH_TRANSFORMS:
            if len(args) != 2 or not args[0].isdigit():
                raise SourceSpecError(
                    f"{self.table}: {name}() nhận (số nguyên, cột), "
                    f"vd {name}(16, order_id). Đang có {args}."
                )
        else:
            raise SourceSpecError(
                f"{self.table}: không biết hàm partition '{name}'. "
                f"Hỗ trợ: {sorted(_TIME_TRANSFORMS + _WIDTH_TRANSFORMS)}."
            )
        return name, args

    def partition_columns(self) -> Optional[List]:
        """Dựng danh sách cột partition cho Iceberg writer.

        Gọi lúc CHẠY chứ không lúc import: F.months()/F.bucket() cần SparkContext đã
        khởi tạo mới dựng được biểu thức.
        """
        parsed = self.parse_partition()
        if parsed is None:
            return None

        from pyspark.sql import functions as F

        name, args = parsed
        if name in _TIME_TRANSFORMS:
            return [getattr(F, name)(args[0])]
        return [getattr(F, name)(int(args[0]), args[1])]


def load_all(sources_file: str) -> Dict[str, SourceSpec]:
    """Đọc toàn bộ sources.yml thành {tên bảng: SourceSpec}."""
    with open(sources_file, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    entries = (raw or {}).get("sources") or []
    if not entries:
        raise SourceSpecError(f"{sources_file}: không có mục `sources` nào.")

    specs = {}
    for entry in entries:
        table = entry.get("table")
        if not table:
            raise SourceSpecError(f"{sources_file}: có mục thiếu trường `table`: {entry}")
        specs[table] = SourceSpec(
            table=table,
            type=entry.get("type", "csv"),
            partition_by=entry.get("partition_by"),
            extra_metrics=entry.get("extra_metrics") or {},
        )
    return specs


def load(table: str, sources_file: str) -> SourceSpec:
    specs = load_all(sources_file)
    if table not in specs:
        raise SourceSpecError(
            f"Bảng '{table}' chưa khai trong {sources_file}. "
            f"Các bảng đã khai: {sorted(specs)}"
        )
    return specs[table]
