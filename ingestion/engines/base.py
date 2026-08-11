"""
Hợp đồng mà mọi execution engine của tầng ingestion phải thoả.

Mục đích của interface này KHÔNG phải là trừu tượng hoá cho đẹp — mà là để
`common/job.py` (nơi mô tả *pipeline làm gì*) không biết gì về DuckDB hay Spark, và
nhờ đó cùng một luồng chạy được ở cả dev lẫn prod.

Ranh giới cố ý:

    job.py       BIẾT: đọc nguồn -> chạy SQL -> ghi Iceberg -> đếm -> log
                 KHÔNG BIẾT: engine nào, đọc bằng cách gì, ghi bằng API nào

    engine       BIẾT: cách nói chuyện với engine của mình
                 KHÔNG BIẾT: bảng nào, cột nào, luật chất lượng nào

Toàn bộ business logic nằm ở ingestion/bronze_specs/bronze_<bảng>.sql — engine chỉ
nhận một chuỗi SQL đã render và thực thi nó.

Interface CỐ TÌNH nhỏ (5 method). Thêm method chỉ khi có nhu cầu thật, không thêm
trước — abstraction thừa còn tệ hơn abstraction thiếu.
"""
from abc import ABC, abstractmethod
from typing import List, Optional, Tuple


class IngestionError(RuntimeError):
    """Lỗi trong lúc ingest — phân biệt với lỗi cấu hình/khai báo."""


class UnknownEngineError(ValueError):
    """INGESTION_ENGINE khai một tên không có engine tương ứng."""


class IngestionEngine(ABC):
    """Engine thực thi ingestion. Dùng như context manager để chắc chắn đóng tài nguyên.

    Vòng đời:
        with engines.create("duckdb") as engine:
            engine.create_namespace("iceberg.bronze")
            engine.register_landing_source(view, table, columns)
            engine.write_table_from_sql(sql, target, partition)
    """

    #: Tên engine, dùng trong log
    name: str = "base"

    # ---- Vòng đời -----------------------------------------------------------
    def __enter__(self) -> "IngestionEngine":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()

    @abstractmethod
    def start(self) -> None:
        """Mở kết nối / dựng session. Gọi trước mọi thao tác khác."""

    @abstractmethod
    def stop(self) -> None:
        """Đóng tài nguyên. PHẢI an toàn khi gọi cả lúc start() đã hỏng giữa chừng."""

    # ---- Thao tác -----------------------------------------------------------
    @abstractmethod
    def create_namespace(self, namespace: str) -> None:
        """Tạo namespace trong catalog nếu chưa có."""

    @abstractmethod
    def register_landing_source(
        self, view_name: str, table: str, columns: dict
    ) -> None:
        """Đăng ký dữ liệu landing của `table` dưới tên quan hệ `view_name`.

        `columns` là {tên cột: kiểu trung lập} khai trong model SQL. Engine áp schema
        này lên dữ liệu đọc được — để hai engine cho ra CÙNG kiểu dữ liệu ở bronze,
        không phụ thuộc vào engine đoán kiểu từ file.
        """

    @abstractmethod
    def write_table_from_sql(
        self,
        sql: str,
        target_table: str,
        partition: Optional[Tuple[str, List[str]]],
    ) -> None:
        """Chạy `sql` rồi GHI ĐÈ toàn bộ `target_table` bằng kết quả.

        `partition` là kết quả của SourceSpec.parse_partition(): (tên hàm, tham số),
        vd ("months", ["order_date"]) hoặc ("bucket", ["16", "order_id"]); None nghĩa
        là bảng không partition. Mỗi engine tự dịch sang cú pháp Iceberg của mình.
        """

    @abstractmethod
    def count(self, table: str) -> int:
        """Đếm toàn bộ số dòng của một bảng."""

    @abstractmethod
    def count_where(self, table: str, condition: str) -> int:
        """Đếm số dòng thoả một điều kiện SQL."""
