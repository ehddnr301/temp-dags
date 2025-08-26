{% macro load_base_date_kst() %}
  {#
    증분 적재 기준일(KST)을 반환.
    - var('load_base_date_kst')가 제공되면 해당 값을 DATE로 캐스팅
    - 미제공 시 현재일 기준 전일
  #}
  {% if var('load_base_date_kst', none) is not none %}
    cast('{{ var('load_base_date_kst') }}' as date)
  {% else %}
    date_add('day', -1, current_date)
  {% endif %}
{% endmacro %}

{% macro delete_by_base_date(base_date_expr=None) %}
  {#
    대상 모델 테이블에서 base_date = expr 행을 삭제.
    테이블이 아직 없으면 noop.
  #}
  {% set expr = base_date_expr if base_date_expr is not none else load_base_date_kst() %}
  {% if execute %}
    {% set target = this %}
    {% set rel = adapter.get_relation(database=target.database, schema=target.schema, identifier=target.identifier) %}
  {% endif %}
  {% if execute and rel is not none %}
    delete from {{ this }} where base_date = {{ expr }}
  {% else %}
    select 1 -- noop: relation not exists
  {% endif %}
{% endmacro %}


