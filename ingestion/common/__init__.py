"""
Hạ tầng dùng chung của tầng ingestion — độc lập với execution engine.

Package này CHỈ chứa phần kỹ thuật: đọc cấu hình từ env, đọc khai báo nguồn, đọc model
bronze dùng chung với dbt, logging, phân loại lỗi, và khung điều phối chung.

Nó KHÔNG chứa business logic của bất kỳ bảng nào, và KHÔNG biết engine nào đang chạy.
Schema, chuẩn hoá, luật chất lượng, cột dẫn xuất nằm ở
transforms/models/bronze/bronze_<bảng>.sql — cùng file mà dbt đọc. Phần "chạy bằng gì"
nằm ở ingestion/engines/. Đó là toàn bộ ý tưởng: một bản logic, hai engine thực thi.

    config     : nơi duy nhất biết catalog / namespace / endpoint / đường dẫn
    spec       : đọc sources.yml — cấu hình ingest mà dbt không cần
    sql_model  : đọc model bronze dùng chung với dbt, render cho engine bất kỳ
    io         : SEAM NGUỒN — csv, và các loại khác khi thêm reader (loader dùng)
    job        : khung điều phối: đọc landing -> chạy SQL -> ghi Iceberg -> đếm
    logging    : logger cho người + sự kiện JSON cho máy
    errors     : phân loại lỗi + exit code để orchestrator biết có nên retry không

File này CỐ Ý không re-export gì. Import lại submodule ở đây sẽ kéo dependency nặng vào
mọi lần `import common`, và khi đó tests/ lẫn lint trên CI đều phải cài cả bộ Spark chỉ
để kiểm mấy chuỗi khai báo. Cứ import thẳng module cần dùng:

    from common import config, spec, sql_model     # nhẹ, không cần Spark/DuckDB
    from common import job                         # kéo theo engine đang chọn
"""
