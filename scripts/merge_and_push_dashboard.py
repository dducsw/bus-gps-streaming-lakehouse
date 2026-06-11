import os
import sys
import zipfile
import shutil
import json
import yaml

# Fix encoding for Windows console (cp1252 -> utf-8)
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
import requests

def save_yaml(data, file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w', encoding='utf-8') as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)

def main():
    workspace_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    superset_dir = os.path.join(workspace_dir, "dashboard", "superset")
    target_dir = os.path.join(superset_dir, "dashboard_bus_analysis")
    
    print(f"Workspace Dir: {workspace_dir}")
    print(f"Target Dir: {target_dir}")
    
    # 0. Backup old target directory first
    backup_dir = os.path.join(superset_dir, "dashboard_bus_analysis_old_backup")
    if os.path.exists(backup_dir):
        shutil.rmtree(backup_dir)
    if os.path.exists(target_dir):
        shutil.copytree(target_dir, backup_dir)
        # Clear target directory to start fresh
        shutil.rmtree(target_dir)
        
    os.makedirs(target_dir, exist_ok=True)
    
    # Define UUIDs and Constants
    DB_UUID = "124e7ab5-39a6-4276-8f5d-cb99b0f793a5"
    DS_TRIP_UUID = "e09aea66-fa2c-4236-b955-b6268d1bbf36"
    DS_GPS_UUID = "66001989-e88d-4d5a-b893-a48af89419fd"
    DASHBOARD_UUID = "bdbe2ce4-09f5-4064-8140-75a722e98578"
    
    # 1. Generate Database YAML
    db_data = {
        "database_name": "Lakehouse",
        "sqlalchemy_uri": "trino://admin@trino:8080/catalog_iceberg",
        "cache_timeout": None,
        "expose_in_sqllab": True,
        "allow_run_async": False,
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_dml": False,
        "allow_file_upload": False,
        "extra": {
            "allows_virtual_table_explore": True
        },
        "impersonate_user": False,
        "configuration_method": "sqlalchemy_form",
        "uuid": DB_UUID,
        "version": "1.0.0"
    }
    save_yaml(db_data, os.path.join(target_dir, "databases", "Lakehouse.yaml"))
    
    # 2. Generate Dataset 1 (trip_summary) YAML
    ds_trip_data = {
        "table_name": "trip_summary",
        "main_dttm_col": "date",
        "description": None,
        "default_endpoint": None,
        "offset": 0,
        "cache_timeout": None,
        "catalog": "catalog_iceberg",
        "schema": "bus_gold",
        "sql": (
            "SELECT \n"
            "  *,\n"
            "  true as is_completed,\n"
            "  trip_duration_minutes as duration_minutes,\n"
            "  (case when trip_duration_minutes > 0 then (60.0 * total_distance_km / trip_duration_minutes) else 0.0 end) as avg_speed,\n"
            "  HOUR(AT_TIMEZONE(start_time, 'Asia/Ho_Chi_Minh')) as hour_vn\n"
            "FROM catalog_iceberg.bus_gold.trip_summary"
        ),
        "params": None,
        "template_params": None,
        "filter_select_enabled": True,
        "fetch_values_predicate": None,
        "extra": None,
        "normalize_columns": False,
        "always_filter_main_dttm": False,
        "folders": None,
        "uuid": DS_TRIP_UUID,
        "metrics": [
            {
                "metric_name": "count",
                "verbose_name": "COUNT(*)",
                "metric_type": "count",
                "expression": "COUNT(*)",
                "description": None,
                "d3format": None,
                "currency": None,
                "extra": None,
                "warning_text": None
            }
        ],
        "columns": [
            {"column_name": "date", "verbose_name": None, "is_dttm": True, "is_active": True, "type": "DATE", "groupby": True, "filterable": True, "expression": None, "description": None, "python_date_format": None, "extra": None},
            {"column_name": "vehicle", "verbose_name": None, "is_dttm": False, "is_active": True, "type": "VARCHAR", "groupby": True, "filterable": True, "expression": None, "description": None, "python_date_format": None, "extra": None},
            {"column_name": "route_id", "verbose_name": None, "is_dttm": False, "is_active": True, "type": "INTEGER", "groupby": True, "filterable": True, "expression": None, "description": None, "python_date_format": None, "extra": None},
            {"column_name": "route_no", "verbose_name": None, "is_dttm": False, "is_active": True, "type": "VARCHAR", "groupby": True, "filterable": True, "expression": None, "description": None, "python_date_format": None, "extra": None},
            {"column_name": "trip_id", "verbose_name": None, "is_dttm": False, "is_active": True, "type": "INTEGER", "groupby": True, "filterable": True, "expression": None, "description": None, "python_date_format": None, "extra": None},
            {"column_name": "start_time", "verbose_name": None, "is_dttm": True, "is_active": True, "type": "TIMESTAMP(6) WITH TIME ZONE", "groupby": True, "filterable": True, "expression": None, "description": None, "python_date_format": None, "extra": None},
            {"column_name": "end_time", "verbose_name": None, "is_dttm": True, "is_active": True, "type": "TIMESTAMP(6) WITH TIME ZONE", "groupby": True, "filterable": True, "expression": None, "description": None, "python_date_format": None, "extra": None},
            {"column_name": "trip_duration_minutes", "verbose_name": None, "is_dttm": False, "is_active": True, "type": "DOUBLE", "groupby": True, "filterable": True, "expression": None, "description": None, "python_date_format": None, "extra": None},
            {"column_name": "total_distance_km", "verbose_name": None, "is_dttm": False, "is_active": True, "type": "DOUBLE", "groupby": True, "filterable": True, "expression": None, "description": None, "python_date_format": None, "extra": None},
            {"column_name": "direction", "verbose_name": None, "is_dttm": False, "is_active": True, "type": "VARCHAR", "groupby": True, "filterable": True, "expression": None, "description": None, "python_date_format": None, "extra": None},
            {"column_name": "confidence_score", "verbose_name": None, "is_dttm": False, "is_active": True, "type": "VARCHAR", "groupby": True, "filterable": True, "expression": None, "description": None, "python_date_format": None, "extra": None},
            {"column_name": "updated_at", "verbose_name": None, "is_dttm": True, "is_active": True, "type": "TIMESTAMP(6) WITH TIME ZONE", "groupby": True, "filterable": True, "expression": None, "description": None, "python_date_format": None, "extra": None},
            {"column_name": "is_completed", "verbose_name": None, "is_dttm": False, "is_active": True, "type": "BOOLEAN", "groupby": True, "filterable": True, "expression": None, "description": None, "python_date_format": None, "extra": None},
            {"column_name": "duration_minutes", "verbose_name": None, "is_dttm": False, "is_active": True, "type": "INTEGER", "groupby": True, "filterable": True, "expression": None, "description": None, "python_date_format": None, "extra": None},
            {"column_name": "avg_speed", "verbose_name": None, "is_dttm": False, "is_active": True, "type": "DOUBLE", "groupby": True, "filterable": True, "expression": None, "description": None, "python_date_format": None, "extra": None},
            {"column_name": "hour_vn", "verbose_name": "Giờ trong ngày (VN)", "is_dttm": False, "is_active": True, "type": "INTEGER", "groupby": True, "filterable": True, "expression": "HOUR(AT_TIMEZONE(start_time, 'Asia/Ho_Chi_Minh'))", "description": "Giờ khởi hành theo giờ Việt Nam (GMT+7), range 0-23", "python_date_format": None, "extra": None}
        ],
        "version": "1.0.0",
        "database_uuid": DB_UUID
    }
    save_yaml(ds_trip_data, os.path.join(target_dir, "datasets", "Lakehouse", "trip_summary_1.yaml"))
    
    # 3. Generate Dataset 2 (route_analysis -> gps_stats_overview) YAML
    ds_gps_data = {
        "table_name": "route analysis",
        "main_dttm_col": "date",
        "description": None,
        "default_endpoint": None,
        "offset": 0,
        "cache_timeout": None,
        "catalog": "catalog_iceberg",
        "schema": "bus_gold",
        "sql": (
            "SELECT *,\n"
            "  hour as hour_vn\n"
            "FROM catalog_iceberg.bus_gold.gps_stats_overview"
        ),
        "params": None,
        "template_params": None,
        "filter_select_enabled": True,
        "fetch_values_predicate": None,
        "extra": None,
        "normalize_columns": False,
        "always_filter_main_dttm": True,
        "folders": None,
        "uuid": DS_GPS_UUID,
        "metrics": [
            {
                "metric_name": "count",
                "verbose_name": "COUNT(*)",
                "metric_type": "count",
                "expression": "COUNT(*)",
                "description": None,
                "d3format": None,
                "currency": None,
                "extra": None,
                "warning_text": None
            }
        ],
        "columns": [
            {"column_name": "date", "verbose_name": None, "is_dttm": True, "is_active": True, "type": "DATE", "groupby": True, "filterable": True, "expression": "", "description": None, "python_date_format": None, "extra": None},
            {"column_name": "vehicle", "verbose_name": None, "is_dttm": False, "is_active": True, "type": "VARCHAR", "groupby": True, "filterable": True, "expression": "", "description": None, "python_date_format": None, "extra": None},
            {"column_name": "route_no", "verbose_name": None, "is_dttm": False, "is_active": True, "type": "VARCHAR", "groupby": True, "filterable": True, "expression": "", "description": None, "python_date_format": None, "extra": None},
            {"column_name": "hour", "verbose_name": None, "is_dttm": False, "is_active": True, "type": "INTEGER", "groupby": True, "filterable": True, "expression": "", "description": None, "python_date_format": None, "extra": None},
            {"column_name": "day_of_week", "verbose_name": None, "is_dttm": False, "is_active": True, "type": "INTEGER", "groupby": True, "filterable": True, "expression": "", "description": None, "python_date_format": None, "extra": None},
            {"column_name": "day_type", "verbose_name": None, "is_dttm": False, "is_active": True, "type": "VARCHAR", "groupby": True, "filterable": True, "expression": "", "description": None, "python_date_format": None, "extra": None},
            {"column_name": "is_peak_hour", "verbose_name": None, "is_dttm": False, "is_active": True, "type": "INTEGER", "groupby": True, "filterable": True, "expression": "", "description": None, "python_date_format": None, "extra": None},
            {"column_name": "speed", "verbose_name": None, "is_dttm": False, "is_active": True, "type": "DOUBLE", "groupby": True, "filterable": True, "expression": "", "description": None, "python_date_format": None, "extra": None},
            {"column_name": "speed_level", "verbose_name": None, "is_dttm": False, "is_active": True, "type": "VARCHAR", "groupby": True, "filterable": True, "expression": "", "description": None, "python_date_format": None, "extra": None},
            {"column_name": "is_moving", "verbose_name": None, "is_dttm": False, "is_active": True, "type": "INTEGER", "groupby": True, "filterable": True, "expression": "", "description": None, "python_date_format": None, "extra": None},
            {"column_name": "is_stopped", "verbose_name": None, "is_dttm": False, "is_active": True, "type": "INTEGER", "groupby": True, "filterable": True, "expression": "", "description": None, "python_date_format": None, "extra": None},
            {"column_name": "x", "verbose_name": None, "is_dttm": False, "is_active": True, "type": "DOUBLE", "groupby": True, "filterable": True, "expression": "", "description": None, "python_date_format": None, "extra": None},
            {"column_name": "y", "verbose_name": None, "is_dttm": False, "is_active": True, "type": "DOUBLE", "groupby": True, "filterable": True, "expression": "", "description": None, "python_date_format": None, "extra": None},
            {"column_name": "updated_at", "verbose_name": None, "is_dttm": True, "is_active": True, "type": "TIMESTAMP(6) WITH TIME ZONE", "groupby": True, "filterable": True, "expression": "", "description": None, "python_date_format": None, "extra": None},
            {"column_name": "hour_vn", "verbose_name": "Giờ trong ngày (VN)", "is_dttm": False, "is_active": True, "type": "INTEGER", "groupby": True, "filterable": True, "expression": "", "description": "Giờ GPS theo giờ Việt Nam (GMT+7), range 0-23", "python_date_format": None, "extra": None}
        ],
        "version": "1.0.0",
        "database_uuid": DB_UUID
    }
    save_yaml(ds_gps_data, os.path.join(target_dir, "datasets", "Lakehouse", "route_analysis_2.yaml"))
    
    # 3. Generate 13 Charts
    # Helper to generate big number charts
    def make_kpi(slice_name, slice_id, uuid, ds_uuid, metric_sql, format_str=None):
        return {
            "slice_name": slice_name,
            "viz_type": "big_number_total",
            "params": {
                "datasource": "1__table" if ds_uuid == DS_TRIP_UUID else "2__table",
                "viz_type": "big_number_total",
                "slice_id": slice_id,
                "metric": {
                    "expressionType": "SQL",
                    "sqlExpression": metric_sql,
                    "column": None,
                    "aggregate": None,
                    "datasourceWarning": False,
                    "hasCustomLabel": False,
                    "label": metric_sql
                },
                "adhoc_filters": [
                    {"clause": "WHERE", "subject": "date", "operator": "TEMPORAL_RANGE", "comparator": "No filter", "expressionType": "SIMPLE"}
                ] + ([{"clause": "WHERE", "expressionType": "SQL", "sqlExpression": "speed > 0", "filterOptionName": "filter_moving_only"}] if "speed" in metric_sql.lower() else []),
                "header_font_size": 0.4,
                "subtitle_font_size": 0.15,
                "show_metric_name": False,
                "y_axis_format": format_str or "SMART_NUMBER",
                "time_format": "smart_date"
            },
            "uuid": uuid,
            "version": "1.0.0",
            "dataset_uuid": ds_uuid
        }
        
    # Generate KPIs
    save_yaml(make_kpi("Tổng số chuyến xe", 101, "5f17d312-d85c-485a-8bc5-01e411b43101", DS_TRIP_UUID, "COUNT(trip_id)"), os.path.join(target_dir, "charts", "kpi_total_trips.yaml"))
    save_yaml(make_kpi("Tuyến xe hoạt động", 102, "5f17d312-d85c-485a-8bc5-01e411b43102", DS_TRIP_UUID, "COUNT(DISTINCT route_no)"), os.path.join(target_dir, "charts", "kpi_active_routes.yaml"))
    save_yaml(make_kpi("Số xe vận hành", 103, "5f17d312-d85c-485a-8bc5-01e411b43103", DS_TRIP_UUID, "COUNT(DISTINCT vehicle)"), os.path.join(target_dir, "charts", "kpi_active_buses.yaml"))
    save_yaml(make_kpi("Tổng quãng đường (km)", 104, "5f17d312-d85c-485a-8bc5-01e411b43104", DS_TRIP_UUID, "SUM(total_distance_km)", ",.1f"), os.path.join(target_dir, "charts", "kpi_total_distance.yaml"))
    save_yaml(make_kpi("Tốc độ trung bình (km/h)", 105, "5f17d312-d85c-485a-8bc5-01e411b43105", DS_GPS_UUID, "AVG(speed)", ",.1f"), os.path.join(target_dir, "charts", "kpi_avg_speed.yaml"))
    save_yaml(make_kpi("Thời gian di chuyển trung bình (phút)", 106, "5f17d312-d85c-485a-8bc5-01e411b43106", DS_TRIP_UUID, "AVG(trip_duration_minutes)", ",.1f"), os.path.join(target_dir, "charts", "kpi_avg_duration.yaml"))
    
    # 111. Chuyến xe theo tuyến (Bar Chart)
    chart_111 = {
        "slice_name": "Biểu đồ chuyến xe theo tuyến",
        "viz_type": "echarts_timeseries_bar",
        "params": {
            "datasource": "1__table",
            "viz_type": "echarts_timeseries_bar",
            "x_axis": "route_no",
            "time_grain_sqla": "P1D",
            "xAxisForceCategorical": True,
            "x_axis_sort_asc": True,
            "metrics": [{
                "expressionType": "SQL",
                "sqlExpression": "COUNT(trip_id)",
                "label": "Tổng số chuyến"
            }],
            "groupby": [],
            "adhoc_filters": [
                {"clause": "WHERE", "subject": "date", "operator": "TEMPORAL_RANGE", "comparator": "No filter", "expressionType": "SIMPLE"}
            ],
            "row_limit": 1000,
            "show_legend": True,
            "y_axis_format": "SMART_NUMBER",
            "rich_tooltip": True
        },
        "uuid": "5f17d312-d85c-485a-8bc5-01e411b43111",
        "version": "1.0.0",
        "dataset_uuid": DS_TRIP_UUID
    }
    save_yaml(chart_111, os.path.join(target_dir, "charts", "chart_trips_by_route.yaml"))
    
    # 112. Tỉ lệ chiều vận hành (Pie Chart)
    chart_112 = {
        "slice_name": "Tỉ lệ chiều vận hành",
        "viz_type": "pie",
        "params": {
            "datasource": "1__table",
            "viz_type": "pie",
            "groupby": ["direction"],
            "metric": {
                "expressionType": "SQL",
                "sqlExpression": "COUNT(trip_id)",
                "label": "Số chuyến"
            },
            "adhoc_filters": [
                {"clause": "WHERE", "subject": "date", "operator": "TEMPORAL_RANGE", "comparator": "No filter", "expressionType": "SIMPLE"}
            ],
            "row_limit": 100,
            "show_legend": True,
            "show_labels": True,
            "is_donut": True,
            "color_scheme": "supersetColors"
        },
        "uuid": "5f17d312-d85c-485a-8bc5-01e411b43112",
        "version": "1.0.0",
        "dataset_uuid": DS_TRIP_UUID
    }
    save_yaml(chart_112, os.path.join(target_dir, "charts", "chart_direction_share.yaml"))
    
    # 113. Xu hướng chuyến xe theo giờ (Bar Categorical - fixed timezone)
    # BUG FIX: Trước đây dùng echarts_timeseries_line với x_axis=start_time (UTC) + PT1H
    # => Superset truncate theo giờ UTC -> hiển thị lệch -7h so với giờ VN thực tế.
    # Fix: dùng bar categorical với x_axis=hour_vn (HOUR(AT_TIMEZONE(start_time,'Asia/Ho_Chi_Minh')))
    chart_113 = {
        "slice_name": "Xu hướng chuyến xe theo giờ (Giờ VN)",
        "viz_type": "echarts_timeseries_bar",
        "params": {
            "datasource": "1__table",
            "viz_type": "echarts_timeseries_bar",
            "x_axis": "hour_vn",
            "time_grain_sqla": "P1D",
            "xAxisForceCategorical": True,
            "x_axis_sort_asc": True,
            "x_axis_title": "Giờ trong ngày (GMT+7)",
            "x_axis_title_margin": 30,
            "y_axis_title": "Số chuyến xe",
            "y_axis_title_margin": 30,
            "metrics": [{
                "expressionType": "SQL",
                "sqlExpression": "COUNT(trip_id)",
                "label": "Số chuyến"
            }],
            "groupby": [],
            "adhoc_filters": [
                {"clause": "WHERE", "subject": "date", "operator": "TEMPORAL_RANGE", "comparator": "No filter", "expressionType": "SIMPLE"}
            ],
            "row_limit": 10000,
            "show_legend": True,
            "y_axis_format": "SMART_NUMBER",
            "rich_tooltip": True,
            "tooltipTimeFormat": "smart_date",
            "orientation": "vertical"
        },
        "uuid": "5f17d312-d85c-485a-8bc5-01e411b43113",
        "version": "1.0.0",
        "dataset_uuid": DS_TRIP_UUID
    }
    save_yaml(chart_113, os.path.join(target_dir, "charts", "chart_trips_by_hour.yaml"))
    
    # 118. Tốc độ trung bình theo giờ trong ngày (Line Chart - GPS data)
    chart_118 = {
        "slice_name": "Tốc độ trung bình theo giờ trong ngày (Giờ VN)",
        "viz_type": "echarts_timeseries_line",
        "params": {
            "datasource": "2__table",
            "viz_type": "echarts_timeseries_line",
            "x_axis": "hour_vn",
            "time_grain_sqla": "P1D",
            "xAxisForceCategorical": True,
            "x_axis_sort_asc": True,
            "x_axis_title": "Giờ trong ngày (GMT+7)",
            "x_axis_title_margin": 30,
            "y_axis_title": "Tốc độ trung bình (km/h)",
            "y_axis_title_margin": 30,
            "metrics": [{
                "expressionType": "SQL",
                "sqlExpression": "AVG(speed)",
                "label": "Tốc độ TB (km/h)"
            }],
            "groupby": ["day_type"],
            "adhoc_filters": [
                {"clause": "WHERE", "subject": "date", "operator": "TEMPORAL_RANGE", "comparator": "No filter", "expressionType": "SIMPLE"},
                {"clause": "WHERE", "expressionType": "SQL", "sqlExpression": "speed > 0", "filterOptionName": "filter_moving_only"}
            ],
            "row_limit": 10000,
            "show_legend": True,
            "y_axis_format": ",.1f",
            "rich_tooltip": True
        },
        "uuid": "5f17d312-d85c-485a-8bc5-01e411b43118",
        "version": "1.0.0",
        "dataset_uuid": DS_GPS_UUID
    }
    save_yaml(chart_118, os.path.join(target_dir, "charts", "chart_speed_by_hour.yaml"))
    
    # 114. Phân bố mức độ tốc độ (Pie Chart)
    chart_114 = {
        "slice_name": "Phân bố mức độ tốc độ",
        "viz_type": "pie",
        "params": {
            "datasource": "2__table",
            "viz_type": "pie",
            "groupby": ["speed_level"],
            "metric": "count",
            "adhoc_filters": [
                {"clause": "WHERE", "subject": "date", "operator": "TEMPORAL_RANGE", "comparator": "No filter", "expressionType": "SIMPLE"}
            ],
            "row_limit": 100,
            "show_legend": True,
            "show_labels": True,
            "is_donut": True,
            "color_scheme": "supersetColors"
        },
        "uuid": "5f17d312-d85c-485a-8bc5-01e411b43114",
        "version": "1.0.0",
        "dataset_uuid": DS_GPS_UUID
    }
    save_yaml(chart_114, os.path.join(target_dir, "charts", "chart_speed_level_share.yaml"))
    
    # 115. Tốc độ trung bình theo tuyến (Bar Chart)
    chart_115 = {
        "slice_name": "Tốc độ trung bình theo tuyến",
        "viz_type": "echarts_timeseries_bar",
        "params": {
            "datasource": "2__table",
            "viz_type": "echarts_timeseries_bar",
            "x_axis": "route_no",
            "time_grain_sqla": "P1D",
            "xAxisForceCategorical": True,
            "x_axis_sort_asc": True,
            "metrics": [{
                "expressionType": "SQL",
                "sqlExpression": "AVG(speed)",
                "label": "Tốc độ trung bình (km/h)"
            }],
            "groupby": [],
            "adhoc_filters": [
                {"clause": "WHERE", "subject": "date", "operator": "TEMPORAL_RANGE", "comparator": "No filter", "expressionType": "SIMPLE"},
                {"clause": "WHERE", "expressionType": "SQL", "sqlExpression": "speed > 0", "filterOptionName": "filter_moving_only"}
            ],
            "row_limit": 1000,
            "show_legend": True,
            "y_axis_format": ",.1f",
            "rich_tooltip": True
        },
        "uuid": "5f17d312-d85c-485a-8bc5-01e411b43115",
        "version": "1.0.0",
        "dataset_uuid": DS_GPS_UUID
    }
    save_yaml(chart_115, os.path.join(target_dir, "charts", "chart_avg_speed_by_route.yaml"))
    
    # 116. Bản đồ tọa độ xe vận hành (Deck.gl Scatter)
    chart_116 = {
        "slice_name": "Bản đồ tọa độ xe vận hành",
        "viz_type": "deck_scatter",
        "params": {
            "datasource": "2__table",
            "viz_type": "deck_scatter",
            "slice_id": 116,
            "spatial": {"type": "latlong", "latCol": "y", "lonCol": "x"},
            "row_limit": 15000,
            "filter_nulls": True,
            "adhoc_filters": [
                {"expressionType": "SIMPLE", "subject": "date", "operator": "TEMPORAL_RANGE", "comparator": "No filter", "clause": "WHERE"},
                {"expressionType": "SIMPLE", "subject": "speed", "operator": "IS NOT NULL", "operatorId": "IS_NOT_NULL", "clause": "WHERE"}
            ],
            "mapbox_style": "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
            "autozoom": True,
            "viewport": {"bearing": 0, "latitude": 10.7769, "longitude": 106.7009, "pitch": 0, "zoom": 11},
            "point_radius_fixed": {"type": "metric", "value": {"aggregate": "MAX", "column": {"column_name": "speed", "type": "DOUBLE"}, "expressionType": "SIMPLE", "label": "MAX(speed)"}},
            "point_unit": "square_m",
            "min_radius": 2,
            "max_radius": 40,
            "multiplier": 0.6,
            "legend_position": "tr",
            "legend_format": "SMART_NUMBER",
            "color_scheme_type": "categorical_palette",
            "dimension": "speed_level",
            "color_picker": {"r": 0, "g": 122, "b": 135, "a": 1},
            "color_scheme": "modernSunset",
            "js_columns": [],
            "js_tooltip": "",
            "js_data_mutator": "",
            "js_onclick_href": ""
        },
        "uuid": "5f17d312-d85c-485a-8bc5-01e411b43116",
        "version": "1.0.0",
        "dataset_uuid": DS_GPS_UUID
    }
    save_yaml(chart_116, os.path.join(target_dir, "charts", "chart_stop_hotspots.yaml"))
    
    # 117. Bảng chi tiết chuyến xe (Table)
    chart_117 = {
        "slice_name": "Bảng chi tiết chuyến xe",
        "viz_type": "table",
        "params": {
            "datasource": "1__table",
            "viz_type": "table",
            "groupby": ["date", "vehicle", "route_no", "direction", "start_time", "end_time", "trip_duration_minutes", "total_distance_km", "confidence_score"],
            "metrics": [],
            "adhoc_filters": [
                {"clause": "WHERE", "subject": "date", "operator": "TEMPORAL_RANGE", "comparator": "No filter", "expressionType": "SIMPLE"}
            ],
            "row_limit": 5000,
            "show_totals": False,
            "include_search": True,
            "allow_rearrange_columns": True,
            "server_page_length": 25,
            "server_pagination": True,
            "table_timestamp_format": "smart_date"
        },
        "uuid": "5f17d312-d85c-485a-8bc5-01e411b43117",
        "version": "1.0.0",
        "dataset_uuid": DS_TRIP_UUID
    }
    save_yaml(chart_117, os.path.join(target_dir, "charts", "chart_trip_details_table.yaml"))
    
    print("All 14 charts generated (13 original + chart_118 Tốc độ theo giờ).")
    
    # 4. Generate Dashboard Structure YAML
    dashboard_layout = {
        "dashboard_title": "Bus Route Analysis",
        "description": None,
        "css": "",
        "slug": None,
        "certified_by": "",
        "certification_details": "",
        "published": True,
        "uuid": DASHBOARD_UUID,
        "position": {
            "ROOT_ID": {
                "children": ["GRID_ID"],
                "id": "ROOT_ID",
                "type": "ROOT"
            },
            "GRID_ID": {
                "children": ["TABS-main"],
                "id": "GRID_ID",
                "parents": ["ROOT_ID"],
                "type": "GRID"
            },
            "TABS-main": {
                "children": ["TAB-operational-overview", "TAB-performance-speed", "TAB-operational-details"],
                "id": "TABS-main",
                "meta": {},
                "parents": ["ROOT_ID", "GRID_ID"],
                "type": "TABS"
            },
            # TAB 1
            "TAB-operational-overview": {
                "children": ["ROW-kpis-1", "ROW-charts-1", "ROW-charts-2"],
                "id": "TAB-operational-overview",
                "meta": {
                    "defaultText": "Tab title",
                    "placeholder": "Tab title",
                    "text": "Tổng quan vận hành"
                },
                "parents": ["ROOT_ID", "GRID_ID", "TABS-main"],
                "type": "TAB"
            },
            "ROW-kpis-1": {
                "children": ["CHART-kpi-101", "CHART-kpi-102", "CHART-kpi-103", "CHART-kpi-104"],
                "id": "ROW-kpis-1",
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
                "parents": ["ROOT_ID", "GRID_ID", "TABS-main", "TAB-operational-overview"],
                "type": "ROW"
            },
            "CHART-kpi-101": {
                "children": [], "id": "CHART-kpi-101",
                "meta": {"chartId": 101, "height": 30, "sliceName": "Tổng số chuyến xe", "uuid": "5f17d312-d85c-485a-8bc5-01e411b43101", "width": 3},
                "parents": ["ROOT_ID", "GRID_ID", "TABS-main", "TAB-operational-overview", "ROW-kpis-1"], "type": "CHART"
            },
            "CHART-kpi-102": {
                "children": [], "id": "CHART-kpi-102",
                "meta": {"chartId": 102, "height": 30, "sliceName": "Tuyến xe hoạt động", "uuid": "5f17d312-d85c-485a-8bc5-01e411b43102", "width": 3},
                "parents": ["ROOT_ID", "GRID_ID", "TABS-main", "TAB-operational-overview", "ROW-kpis-1"], "type": "CHART"
            },
            "CHART-kpi-103": {
                "children": [], "id": "CHART-kpi-103",
                "meta": {"chartId": 103, "height": 30, "sliceName": "Số xe vận hành", "uuid": "5f17d312-d85c-485a-8bc5-01e411b43103", "width": 3},
                "parents": ["ROOT_ID", "GRID_ID", "TABS-main", "TAB-operational-overview", "ROW-kpis-1"], "type": "CHART"
            },
            "CHART-kpi-104": {
                "children": [], "id": "CHART-kpi-104",
                "meta": {"chartId": 104, "height": 30, "sliceName": "Tổng quãng đường (km)", "uuid": "5f17d312-d85c-485a-8bc5-01e411b43104", "width": 3},
                "parents": ["ROOT_ID", "GRID_ID", "TABS-main", "TAB-operational-overview", "ROW-kpis-1"], "type": "CHART"
            },
            "ROW-charts-1": {
                "children": ["CHART-bar-111", "CHART-pie-112"],
                "id": "ROW-charts-1",
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
                "parents": ["ROOT_ID", "GRID_ID", "TABS-main", "TAB-operational-overview"],
                "type": "ROW"
            },
            "CHART-bar-111": {
                "children": [], "id": "CHART-bar-111",
                "meta": {"chartId": 111, "height": 60, "sliceName": "Biểu đồ chuyến xe theo tuyến", "uuid": "5f17d312-d85c-485a-8bc5-01e411b43111", "width": 7},
                "parents": ["ROOT_ID", "GRID_ID", "TABS-main", "TAB-operational-overview", "ROW-charts-1"], "type": "CHART"
            },
            "CHART-pie-112": {
                "children": [], "id": "CHART-pie-112",
                "meta": {"chartId": 112, "height": 60, "sliceName": "Tỉ lệ chiều vận hành", "uuid": "5f17d312-d85c-485a-8bc5-01e411b43112", "width": 5},
                "parents": ["ROOT_ID", "GRID_ID", "TABS-main", "TAB-operational-overview", "ROW-charts-1"], "type": "CHART"
            },
            "ROW-charts-2": {
                "children": ["CHART-bar-113"],
                "id": "ROW-charts-2",
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
                "parents": ["ROOT_ID", "GRID_ID", "TABS-main", "TAB-operational-overview"],
                "type": "ROW"
            },
            "CHART-bar-113": {
                "children": [], "id": "CHART-bar-113",
                "meta": {"chartId": 113, "height": 60, "sliceName": "Xu hướng chuyến xe theo giờ (Giờ VN)", "uuid": "5f17d312-d85c-485a-8bc5-01e411b43113", "width": 12},
                "parents": ["ROOT_ID", "GRID_ID", "TABS-main", "TAB-operational-overview", "ROW-charts-2"], "type": "CHART"
            },
            
            # TAB 2
            "TAB-performance-speed": {
                "children": ["ROW-kpis-2", "ROW-charts-3", "ROW-charts-4", "ROW-charts-5"],
                "id": "TAB-performance-speed",
                "meta": {
                    "defaultText": "Tab title",
                    "placeholder": "Tab title",
                    "text": "Hiệu suất & Tốc độ"
                },
                "parents": ["ROOT_ID", "GRID_ID", "TABS-main"],
                "type": "TAB"
            },
            "ROW-kpis-2": {
                "children": ["CHART-kpi-105", "CHART-kpi-106"],
                "id": "ROW-kpis-2",
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
                "parents": ["ROOT_ID", "GRID_ID", "TABS-main", "TAB-performance-speed"],
                "type": "ROW"
            },
            "CHART-kpi-105": {
                "children": [], "id": "CHART-kpi-105",
                "meta": {"chartId": 105, "height": 30, "sliceName": "Tốc độ trung bình (km/h)", "uuid": "5f17d312-d85c-485a-8bc5-01e411b43105", "width": 6},
                "parents": ["ROOT_ID", "GRID_ID", "TABS-main", "TAB-performance-speed", "ROW-kpis-2"], "type": "CHART"
            },
            "CHART-kpi-106": {
                "children": [], "id": "CHART-kpi-106",
                "meta": {"chartId": 106, "height": 30, "sliceName": "Thời gian di chuyển trung bình (phút)", "uuid": "5f17d312-d85c-485a-8bc5-01e411b43106", "width": 6},
                "parents": ["ROOT_ID", "GRID_ID", "TABS-main", "TAB-performance-speed", "ROW-kpis-2"], "type": "CHART"
            },
            "ROW-charts-3": {
                "children": ["CHART-pie-114", "CHART-bar-115"],
                "id": "ROW-charts-3",
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
                "parents": ["ROOT_ID", "GRID_ID", "TABS-main", "TAB-performance-speed"],
                "type": "ROW"
            },
            "CHART-pie-114": {
                "children": [], "id": "CHART-pie-114",
                "meta": {"chartId": 114, "height": 60, "sliceName": "Phân bố mức độ tốc độ", "uuid": "5f17d312-d85c-485a-8bc5-01e411b43114", "width": 5},
                "parents": ["ROOT_ID", "GRID_ID", "TABS-main", "TAB-performance-speed", "ROW-charts-3"], "type": "CHART"
            },
            "CHART-bar-115": {
                "children": [], "id": "CHART-bar-115",
                "meta": {"chartId": 115, "height": 60, "sliceName": "Tốc độ trung bình theo tuyến", "uuid": "5f17d312-d85c-485a-8bc5-01e411b43115", "width": 7},
                "parents": ["ROOT_ID", "GRID_ID", "TABS-main", "TAB-performance-speed", "ROW-charts-3"], "type": "CHART"
            },
            "ROW-charts-4": {
                "children": ["CHART-map-116"],
                "id": "ROW-charts-4",
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
                "parents": ["ROOT_ID", "GRID_ID", "TABS-main", "TAB-performance-speed"],
                "type": "ROW"
            },
            "CHART-map-116": {
                "children": [], "id": "CHART-map-116",
                "meta": {"chartId": 116, "height": 80, "sliceName": "Bản đồ tọa độ xe vận hành", "uuid": "5f17d312-d85c-485a-8bc5-01e411b43116", "width": 12},
                "parents": ["ROOT_ID", "GRID_ID", "TABS-main", "TAB-performance-speed", "ROW-charts-4"], "type": "CHART"
            },
            "ROW-charts-5": {
                "children": ["CHART-line-118"],
                "id": "ROW-charts-5",
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
                "parents": ["ROOT_ID", "GRID_ID", "TABS-main", "TAB-performance-speed"],
                "type": "ROW"
            },
            "CHART-line-118": {
                "children": [], "id": "CHART-line-118",
                "meta": {"chartId": 118, "height": 60, "sliceName": "Tốc độ trung bình theo giờ trong ngày (Giờ VN)", "uuid": "5f17d312-d85c-485a-8bc5-01e411b43118", "width": 12},
                "parents": ["ROOT_ID", "GRID_ID", "TABS-main", "TAB-performance-speed", "ROW-charts-5"], "type": "CHART"
            },
            
            # TAB 3
            "TAB-operational-details": {
                "children": ["ROW-details"],
                "id": "TAB-operational-details",
                "meta": {
                    "defaultText": "Tab title",
                    "placeholder": "Tab title",
                    "text": "Chi tiết chuyến xe"
                },
                "parents": ["ROOT_ID", "GRID_ID", "TABS-main"],
                "type": "TAB"
            },
            "ROW-details": {
                "children": ["CHART-table-117"],
                "id": "ROW-details",
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
                "parents": ["ROOT_ID", "GRID_ID", "TABS-main", "TAB-operational-details"],
                "type": "ROW"
            },
            "CHART-table-117": {
                "children": [], "id": "CHART-table-117",
                "meta": {"chartId": 117, "height": 80, "sliceName": "Bảng chi tiết chuyến xe", "uuid": "5f17d312-d85c-485a-8bc5-01e411b43117", "width": 12},
                "parents": ["ROOT_ID", "GRID_ID", "TABS-main", "TAB-operational-details", "ROW-details"], "type": "CHART"
            }
        },
        "metadata": {
            "color_scheme_domain": [],
            "shared_label_colors": [],
            "map_label_colors": {},
            "label_colors": {},
            "chart_configuration": {},
            "global_chart_configuration": {
                "scope": {
                    "rootPath": ["ROOT_ID"],
                    "excluded": []
                },
                "chartsInScope": [101, 102, 103, 104, 105, 106, 111, 112, 113, 114, 115, 116, 117, 118]
            },
            "color_scheme": "",
            "refresh_frequency": 0,
            "expanded_slices": {},
            "timed_refresh_immune_slices": [],
            "cross_filters_enabled": True,
            "default_filters": "{}",
            "native_filter_configuration": [
                {
                    "id": "NATIVE_FILTER-RouteSelect",
                    "controlValues": {
                        "enableEmptyFilter": False,
                        "defaultToFirstItem": False,
                        "creatable": True,
                        "multiSelect": True,
                        "searchAllOptions": False,
                        "inverseSelection": False
                    },
                    "name": "Chọn tuyến xe",
                    "filterType": "filter_select",
                    "targets": [
                        {
                            "column": {"name": "route_no"},
                            "datasetUuid": DS_TRIP_UUID
                        },
                        {
                            "column": {"name": "route_no"},
                            "datasetUuid": DS_GPS_UUID
                        }
                    ],
                    "defaultDataMask": {
                        "extraFormData": {},
                        "filterState": {},
                        "ownState": {}
                    },
                    "cascadeParentIds": [],
                    "scope": {
                        "rootPath": ["ROOT_ID"],
                        "excluded": []
                    },
                    "type": "NATIVE_FILTER",
                    "description": "",
                    "chartsInScope": [101, 102, 103, 104, 105, 106, 111, 112, 113, 114, 115, 116, 117, 118],
                    "tabsInScope": []
                },
                {
                    "id": "NATIVE_FILTER-DateFilter",
                    "controlValues": {
                        "enableEmptyFilter": False,
                        "defaultToFirstItem": False,
                        "creatable": True,
                        "multiSelect": True,
                        "searchAllOptions": False,
                        "inverseSelection": False
                    },
                    "name": "Chọn ngày",
                    "filterType": "filter_time",
                    "targets": [
                        {
                            "column": {"name": "date"},
                            "datasetUuid": DS_TRIP_UUID
                        },
                        {
                            "column": {"name": "date"},
                            "datasetUuid": DS_GPS_UUID
                        }
                    ],
                    "defaultDataMask": {
                        "extraFormData": {
                            "time_range": "2025-03-21 : 2025-03-24"
                        },
                        "filterState": {
                            "label": "2025-03-21 : 2025-03-24",
                            "value": "2025-03-21 : 2025-03-24"
                        },
                        "ownState": {}
                    },
                    "cascadeParentIds": [],
                    "scope": {
                        "rootPath": ["ROOT_ID"],
                        "excluded": []
                    },
                    "type": "NATIVE_FILTER",
                    "description": "",
                    "chartsInScope": [101, 102, 103, 104, 105, 106, 111, 112, 113, 114, 115, 116, 117, 118],
                    "tabsInScope": []
                }
            ]
        },
        "theme_uuid": None,
        "version": "1.0.0"
    }
    save_yaml(dashboard_layout, os.path.join(target_dir, "dashboards", "Bus_Route_Analysis_1.yaml"))
    
    # Write metadata.yaml
    metadata = {
        "version": "1.0.0",
        "type": "Dashboard",
        "timestamp": "2026-06-08T14:20:00.000000+00:00"
    }
    save_yaml(metadata, os.path.join(target_dir, "metadata.yaml"))
    print("Dashboard structure generated.")
    
    # 5. Zip the target directory
    zip_path = os.path.join(superset_dir, "dashboard_bus_analysis.zip")
    print(f"Creating ZIP archive: {zip_path}...")
    if os.path.exists(zip_path):
        os.remove(zip_path)
        
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(target_dir):
            for file in files:
                file_path = os.path.join(root, file)
                rel_path = os.path.relpath(file_path, target_dir).replace('\\', '/')
                archive_path = f"dashboard_bus_analysis/{rel_path}"
                zipf.write(file_path, archive_path)
                
    print(f"ZIP archive created successfully at: {zip_path}")
    
    # 6. Push to Superset REST API
    superset_url = os.getenv("SUPERSET_URL", "http://localhost:8089")
    username = os.getenv("SUPERSET_USERNAME", "admin")
    password = os.getenv("SUPERSET_PASSWORD", "admin")
    
    print(f"Attempting to push import to Superset at: {superset_url}...")
    session = requests.Session()
    
    # 6.1 Authenticate
    login_url = f"{superset_url}/api/v1/security/login"
    login_data = {
        "username": username,
        "password": password,
        "provider": "db",
        "refresh": True
    }
    
    try:
        r_login = session.post(login_url, json=login_data, timeout=10)
        r_login.raise_for_status()
        tokens = r_login.json()
        access_token = tokens["access_token"]
        print("Authenticated successfully to Superset REST API.")
    except Exception as e:
        print(f"Authentication failed: {e}")
        return
        
    # 6.2 Fetch CSRF
    csrf_url = f"{superset_url}/api/v1/security/csrf_token/"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    try:
        r_csrf = session.get(csrf_url, headers=headers, timeout=10)
        r_csrf.raise_for_status()
        csrf_token = r_csrf.json()["result"]
        print("CSRF token retrieved successfully.")
    except Exception as e:
        print(f"Failed to fetch CSRF token: {e}")
        return
        
    # 6.3 Post ZIP
    import_url = f"{superset_url}/api/v1/dashboard/import/"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "X-CSRFToken": csrf_token
    }
    
    passwords = {
        "databases/Lakehouse.yaml": ""
    }
    
    files = {
        "formData": (os.path.basename(zip_path), open(zip_path, 'rb'), "application/zip")
    }
    data = {
        "passwords": json.dumps(passwords),
        "overwrite": "true"
    }
    
    try:
        r_import = session.post(import_url, headers=headers, files=files, data=data, timeout=30)
        if r_import.status_code == 200:
            print("IMPORT SUCCESS!")
            print(f"Response: {r_import.json()}")
            
            # Fetch IDs and update SQL
            trip_ds_id = None
            route_ds_id = None
            try:
                datasets_url = f"{superset_url}/api/v1/dataset/"
                r_ds = session.get(datasets_url, headers=headers, timeout=10)
                r_ds.raise_for_status()
                datasets_list = r_ds.json().get("result", [])
                for ds in datasets_list:
                    if ds.get("uuid") == DS_TRIP_UUID:
                        trip_ds_id = ds.get("id")
                    elif ds.get("uuid") == DS_GPS_UUID:
                        route_ds_id = ds.get("id")
            except Exception as e:
                print(f"Warning: Failed to fetch datasets list to find IDs: {e}")
                
            put_headers = {
                "Authorization": f"Bearer {access_token}",
                "X-CSRFToken": csrf_token
            }
            
            # Update trip_summary dataset SQL
            if trip_ds_id:
                print(f"Updating trip_summary dataset SQL (ID {trip_ds_id})...")
                dataset_put_url = f"{superset_url}/api/v1/dataset/{trip_ds_id}"
                dataset_data = {
                    "sql": (
                        "SELECT \n"
                        "  *,\n"
                        "  true as is_completed,\n"
                        "  trip_duration_minutes as duration_minutes,\n"
                        "  (case when trip_duration_minutes > 0 then (60.0 * total_distance_km / trip_duration_minutes) else 0.0 end) as avg_speed,\n"
                        "  HOUR(AT_TIMEZONE(start_time, 'Asia/Ho_Chi_Minh')) as hour_vn\n"
                        "FROM catalog_iceberg.bus_gold.trip_summary"
                    )
                }
                try:
                    r_put = session.put(dataset_put_url, headers=put_headers, json=dataset_data, timeout=10)
                    r_put.raise_for_status()
                    print("trip_summary SQL updated successfully.")
                    
                    dataset_refresh_url = f"{superset_url}/api/v1/dataset/{trip_ds_id}/refresh"
                    r_ref = session.put(dataset_refresh_url, headers=put_headers, timeout=10)
                    r_ref.raise_for_status()
                    print("trip_summary columns refreshed successfully.")
                except Exception as e:
                    print(f"Warning: Failed to update or refresh trip_summary: {e}")
                    
            # Update route_analysis dataset SQL
            if route_ds_id:
                print(f"Updating route_analysis dataset SQL (ID {route_ds_id})...")
                dataset_put_url = f"{superset_url}/api/v1/dataset/{route_ds_id}"
                dataset_data = {
                    "sql": (
                        "SELECT *,\n"
                        "  hour as hour_vn\n"
                        "FROM catalog_iceberg.bus_gold.gps_stats_overview"
                    )
                }
                try:
                    r_put = session.put(dataset_put_url, headers=put_headers, json=dataset_data, timeout=10)
                    r_put.raise_for_status()
                    print("route_analysis SQL updated successfully.")
                    
                    dataset_refresh_url = f"{superset_url}/api/v1/dataset/{route_ds_id}/refresh"
                    r_ref = session.put(dataset_refresh_url, headers=put_headers, timeout=10)
                    r_ref.raise_for_status()
                    print("route_analysis columns refreshed successfully.")
                except Exception as e:
                    print(f"Warning: Failed to update or refresh route_analysis: {e}")
                    
        else:
            print(f"Import failed with status code: {r_import.status_code}")
            print(f"Error detail: {r_import.text}")
    except Exception as e:
        print(f"Import request failed: {e}")

if __name__ == "__main__":
    main()
