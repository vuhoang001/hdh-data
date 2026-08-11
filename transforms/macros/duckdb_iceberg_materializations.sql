{#
    Materialization `table` cho DuckDB khi đích là bảng Iceberg.

    VÌ SAO PHẢI GHI ĐÈ MẶC ĐỊNH — nguyên nhân đã cô lập bằng thực nghiệm:

      dbt build một model `table` theo trình tự
          CREATE TABLE <model>__dbt_tmp AS ...   rồi   ALTER TABLE ... RENAME TO <model>
      và gói cả hai vào MỘT transaction. DuckDB-Iceberg từ chối bước rename:

          Catalog Error: This table (x__dbt_tmp) was modified already, can't be renamed!

      Ba phép thử tách bạch nguyên nhân:
          BEGIN; CTAS; RENAME; COMMIT   -> LỖI
          CTAS; RENAME  (autocommit)    -> ok
          DROP; CREATE; DROP; CREATE    -> ok

      Tức vấn đề là transaction, không phải bản thân RENAME. Và vì DuckDB-Iceberg cũng
      chưa hỗ trợ `CREATE OR REPLACE TABLE` ("Please use separate Drop and Create
      Statements"), đường đi khả dĩ duy nhất là DROP rồi CREATE.

    Macro này bám SÁT bản mặc định của dbt (global_project/macros/materializations/
    models/table.sql) và chỉ thay đúng một đoạn: bỏ intermediate + rename + backup,
    thay bằng drop + create thẳng vào target. Giữ nguyên phần còn lại (hooks, grants,
    persist_docs, commit) để hành vi không lệch khỏi dbt ở những chỗ không liên quan.

    LƯU Ý CHO NGƯỜI SỬA SAU: biến chứa SQL của model trong ngữ cảnh materialization tên
    là `sql`, KHÔNG phải `compiled_code`. Dùng nhầm tên sẽ cho ra một câu lệnh rỗng chạy
    im lặng — dbt vẫn báo "OK created" trong khi không có bảng nào được tạo.

    ĐÁNH ĐỔI: mất tính nguyên tử. Bản mặc định giữ bảng cũ nguyên vẹn cho tới lúc rename,
    nên model build hỏng không làm mất bảng đang có; ở đây có một khoảng bảng không tồn
    tại. Chấp nhận được vì macro chỉ áp dụng cho adapter duckdb (môi trường DEV) — target
    prod chạy Trino và dùng materialization mặc định.
#}
{#
    DROP không kèm CASCADE.

    `duckdb__drop_relation` mặc định sinh `drop table if exists <x> cascade`, và
    DuckDB-Iceberg từ chối:

        Not implemented Error: DROP TABLE <table_name> CASCADE is not supported
        for Iceberg tables currently

    Adapter đã có sẵn ngoại lệ tương tự cho ducklake (`adapter.is_ducklake(relation)`)
    nhưng chưa có cho Iceberg, nên ghi đè ở đây.

    Bỏ CASCADE không mất gì trong project này: cascade chỉ cần để dọn các object phụ
    thuộc, mà DuckDB-Iceberg vốn chưa tạo được view — nên không có gì để cascade.

    Lỗi này chỉ lộ ra ở lần build THỨ HAI (lần đầu chưa có bảng cũ để drop), nên nó là
    loại lỗi mà một lần chạy thử duy nhất sẽ bỏ sót.
#}
{% macro duckdb__drop_relation(relation) -%}
    {% call statement('drop_relation', auto_begin=False) -%}
        drop {{ relation.type }} if exists {{ relation }}
    {%- endcall %}
{% endmacro %}


{% materialization table, adapter='duckdb' %}

    {%- set existing_relation = load_cached_relation(this) -%}
    {%- set target_relation = this.incorporate(type='table') -%}
    {%- set grant_config = config.get('grants') -%}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    {#-
        Khác bản mặc định: DROP thẳng target thay vì dựng intermediate rồi rename.

        DROP phải nằm TRƯỚC khi transaction mở (tức trước run_hooks inside_transaction),
        và macro duckdb__drop_relation bên trên dùng auto_begin=False. Nếu drop và create
        rơi vào cùng một transaction, DuckDB-Iceberg báo:

            Not implemented Error: Cannot create table deleted within a transaction

        Đây là biến thể thứ hai của cùng một ràng buộc đã gặp ở RENAME: catalog Iceberg
        không cho tạo lại một bảng vừa bị xoá trong cùng transaction.
    -#}
    {% if existing_relation is not none %}
        {{ adapter.drop_relation(existing_relation) }}
        {#-
            COMMIT NGAY SAU DROP — đây là mấu chốt, và là điều `auto_begin=False` trên
            từng câu lệnh KHÔNG làm được.

            dbt mở `BEGIN` ở đầu mỗi node, TRƯỚC khi materialization chạy (thấy rõ trong
            `dbt --debug`: "On model.hdh_dbt.dim_date: BEGIN" đứng trước cả DROP).
            auto_begin=False chỉ có nghĩa "đừng mở transaction MỚI", không đóng được
            transaction đang mở sẵn. Nên DROP và CREATE vẫn rơi vào cùng một transaction
            và catalog Iceberg từ chối:

                Not implemented Error: Cannot create table deleted within a transaction

            COMMIT ở đây đóng transaction chứa DROP lại. CREATE bên dưới mở transaction
            mới, sạch, không mang theo dấu vết bảng vừa bị xoá.
        -#}
        {{ adapter.commit() }}
    {% endif %}

    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% call statement('main') -%}
        {{ get_create_table_as_sql(False, target_relation, sql) }}
    {%- endcall %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    {% set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) %}
    {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

    {% do persist_docs(target_relation, model) %}

    {{ adapter.commit() }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
