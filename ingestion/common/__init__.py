"""
Hạ tầng dùng chung cho mọi Spark job.

Package này CHỈ chứa phần kỹ thuật: đọc cấu hình từ env, tạo SparkSession, đọc nguồn, ghi
bảng Iceberg, logging, và khung chạy chung của bronze job.

Nó KHÔNG chứa business logic của bất kỳ bảng nào. Schema, chuẩn hoá, luật chất lượng và
cột dẫn xuất nằm ở transforms/models/bronze/bronze_<bảng>.sql — cùng một file mà dbt build
ở môi trường DuckDB. Đó là toàn bộ ý tưởng: một bản logic, hai engine thực thi.

    config     : nơi duy nhất biết tên catalog / namespace / thư mục dữ liệu (đọc từ .env)
    sql_model  : đọc model bronze dùng chung với dbt, render cho Spark   [không cần pyspark]
    spec       : đọc sources.yml — cấu hình ingest mà dbt không cần      [không cần pyspark]
    job        : khung chạy chung: đọc nguồn -> chạy SQL -> ghi Iceberg
    session    : SparkSession + logger
    io         : đọc nguồn (csv, và các loại khác khi thêm reader)
    iceberg    : ghi bảng Iceberg

File này CỐ Ý không re-export gì. Import lại các submodule ở đây sẽ kéo pyspark vào mọi
lần `import common`, và khi đó tests/ lẫn job lint trên CI đều phải cài cả bộ Spark chỉ để
kiểm mấy chuỗi khai báo. Cứ import thẳng module cần dùng:

    from common import config, spec, sql_model     # nhẹ, không cần Spark
    from common import job                         # cần pyspark
"""
