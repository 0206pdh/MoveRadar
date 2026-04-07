-- ============================================================
-- MoveRadar v2.0 파이프라인
-- 신호 3개: 시세 + 전입인구 + 통신 개통 (아정네트웍스 V01)
-- LLM 프롬프트: V06 렌탈 트렌드 + V09/V11 콜센터 컨텍스트 반영
-- 커버리지: 서울 전체 (SD = '서울')
-- ============================================================

CREATE DATABASE IF NOT EXISTS MOVERADAR;
USE DATABASE MOVERADAR;
CREATE SCHEMA IF NOT EXISTS PUBLIC;
USE SCHEMA PUBLIC;

-- 편의상 DB 이름 alias (실제 SQL에서는 전체 이름 사용)
-- TELECOM_DB = SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.TELECOM_INSIGHTS


-- ============================================================
-- STEP 0: 컬럼명 사전 확인 (처음 한 번만 — 결과 보고 아래 스텝 수정)
-- ============================================================

-- 0-1. 아파트 시세 컬럼 확인
SELECT * FROM KOREA_REAL_ESTATE_APARTMENT_MARKET_INTELLIGENCE.HACKATHON_2026.REGION_APT_RICHGO_MARKET_PRICE_M_H
WHERE SGG = '서초구' AND REGION_LEVEL = 'emd' LIMIT 3;

-- 0-2. 전입인구 컬럼 확인
SELECT * FROM KOREA_REAL_ESTATE_APARTMENT_MARKET_INTELLIGENCE.HACKATHON_2026.REGION_POPULATION_MOVEMENT
WHERE MOVEMENT_TYPE = '전입' AND REGION_LEVEL = 'sgg' AND SD = '서울' LIMIT 3;

-- 0-3. ASSET_INCOME_INFO 컬럼 확인
SELECT * FROM SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.ASSET_INCOME_INFO
LIMIT 3;

-- 0-4. M_SCCO_MST 컬럼 확인 + 서울 커버리지 확인
SELECT DISTINCT CITY_KOR_NAME
FROM SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.M_SCCO_MST
ORDER BY 1;
-- ⚠️ 결과에 25개 서울 SGG 다 있으면 STEP 2 WHERE 절 제거 가능

-- 0-5. 아정네트웍스 뷰 목록 확인 (실제 뷰명 확인)
SHOW VIEWS IN SCHEMA SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.TELECOM_INSIGHTS;
-- ⚠️ 뷰명이 'V01' 인지 'V01_MONTHLY...' 인지 확인 후 아래 SQL 수정

-- 0-6. V01 실제 INSTALL_STATE 값 확인 (필터 없이)
SELECT DISTINCT INSTALL_STATE
FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.TELECOM_INSIGHTS.V01_MONTHLY_REGIONAL_CONTRACT_STATS
ORDER BY 1;
-- ⚠️ 결과에서 서울 표기 확인 ('서울특별시' / '서울' / '서울시' 중 어떤 값인지)

-- 0-6b. V01 데이터 샘플 (무필터)
SELECT * FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.TELECOM_INSIGHTS.V01_MONTHLY_REGIONAL_CONTRACT_STATS
LIMIT 5;

-- 0-7. V01 INSTALL_STATE별 INSTALL_CITY 목록 + 날짜 범위
SELECT INSTALL_STATE, INSTALL_CITY,
       MIN(YEAR_MONTH) AS MIN_YM, MAX(YEAR_MONTH) AS MAX_YM, COUNT(*) AS cnt
FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.TELECOM_INSIGHTS.V01_MONTHLY_REGIONAL_CONTRACT_STATS
GROUP BY 1, 2 ORDER BY 1, cnt DESC LIMIT 50;

-- 0-8. V06_RENTAL_CATEGORY_TRENDS 샘플
SELECT RENTAL_MAIN_CATEGORY, RENTAL_SUB_CATEGORY, SUM(OPEN_COUNT) AS total_open
FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.TELECOM_INSIGHTS.V06_RENTAL_CATEGORY_TRENDS
GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 20;

-- 0-9. V09_MONTHLY_CALL_STATS 최신 데이터 확인
SELECT * FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.TELECOM_INSIGHTS.V09_MONTHLY_CALL_STATS
ORDER BY YEAR_MONTH DESC LIMIT 10;

-- 0-10. TMAP 서울 교통량 날짜 범위 + 커버리지 확인
-- ⚠️ DATA_DATE 범위가 18개월 미만이면 STEP 1-7/1-8/3-4/4-4 건너뛰고 TMAP을 LLM 컨텍스트 전용으로 사용
SELECT
    COUNT(*)           AS total_rows,
    MIN(DATA_DATE)     AS min_dt,
    MAX(DATA_DATE)     AS max_dt,
    DATEDIFF('MONTH', MIN(DATA_DATE), MAX(DATA_DATE)) AS months_available
FROM KOREA_TRAFFIC_SPEED__VOLUME_DATA__TMAP_NATIONWIDE_COVERAGE.TRAFFIC.TMAP_TRAFFIC_VOLUME;

-- 서울 도로명 패턴 커버리지 확인
SELECT
    CASE
        WHEN ROAD_NAME LIKE '%강남%' OR ST_NODE_NAME LIKE '%강남%' OR ED_NODE_NAME LIKE '%강남%' THEN '강남구'
        WHEN ROAD_NAME LIKE '%서초%' OR ST_NODE_NAME LIKE '%서초%' OR ED_NODE_NAME LIKE '%서초%' THEN '서초구'
        WHEN ROAD_NAME LIKE '%영등포%' OR ST_NODE_NAME LIKE '%영등포%' OR ED_NODE_NAME LIKE '%영등포%' THEN '영등포구'
        WHEN ROAD_NAME LIKE '%마포%' OR ST_NODE_NAME LIKE '%마포%' OR ED_NODE_NAME LIKE '%마포%' THEN '마포구'
        WHEN ROAD_NAME LIKE '%종로%' OR ST_NODE_NAME LIKE '%종로%' OR ED_NODE_NAME LIKE '%종로%' THEN '종로구'
        WHEN ROAD_NAME LIKE '%중랑%' OR ST_NODE_NAME LIKE '%중랑%' OR ED_NODE_NAME LIKE '%중랑%' THEN '중랑구'
        WHEN ROAD_NAME LIKE '%중구%' OR ST_NODE_NAME LIKE '%중구%' OR ED_NODE_NAME LIKE '%중구%' THEN '중구'
        WHEN ROAD_NAME LIKE '%송파%' OR ST_NODE_NAME LIKE '%송파%' OR ED_NODE_NAME LIKE '%송파%' THEN '송파구'
        WHEN ROAD_NAME LIKE '%강동%' OR ST_NODE_NAME LIKE '%강동%' OR ED_NODE_NAME LIKE '%강동%' THEN '강동구'
        WHEN ROAD_NAME LIKE '%강서%' OR ST_NODE_NAME LIKE '%강서%' OR ED_NODE_NAME LIKE '%강서%' THEN '강서구'
        WHEN ROAD_NAME LIKE '%강북%' OR ST_NODE_NAME LIKE '%강북%' OR ED_NODE_NAME LIKE '%강북%' THEN '강북구'
        WHEN ROAD_NAME LIKE '%노원%' OR ST_NODE_NAME LIKE '%노원%' OR ED_NODE_NAME LIKE '%노원%' THEN '노원구'
        WHEN ROAD_NAME LIKE '%도봉%' OR ST_NODE_NAME LIKE '%도봉%' OR ED_NODE_NAME LIKE '%도봉%' THEN '도봉구'
        WHEN ROAD_NAME LIKE '%성북%' OR ST_NODE_NAME LIKE '%성북%' OR ED_NODE_NAME LIKE '%성북%' THEN '성북구'
        WHEN ROAD_NAME LIKE '%동대문%' OR ST_NODE_NAME LIKE '%동대문%' OR ED_NODE_NAME LIKE '%동대문%' THEN '동대문구'
        WHEN ROAD_NAME LIKE '%광진%' OR ST_NODE_NAME LIKE '%광진%' OR ED_NODE_NAME LIKE '%광진%' THEN '광진구'
        WHEN ROAD_NAME LIKE '%성동%' OR ST_NODE_NAME LIKE '%성동%' OR ED_NODE_NAME LIKE '%성동%' THEN '성동구'
        WHEN ROAD_NAME LIKE '%용산%' OR ST_NODE_NAME LIKE '%용산%' OR ED_NODE_NAME LIKE '%용산%' THEN '용산구'
        WHEN ROAD_NAME LIKE '%은평%' OR ST_NODE_NAME LIKE '%은평%' OR ED_NODE_NAME LIKE '%은평%' THEN '은평구'
        WHEN ROAD_NAME LIKE '%서대문%' OR ST_NODE_NAME LIKE '%서대문%' OR ED_NODE_NAME LIKE '%서대문%' THEN '서대문구'
        WHEN ROAD_NAME LIKE '%동작%' OR ST_NODE_NAME LIKE '%동작%' OR ED_NODE_NAME LIKE '%동작%' THEN '동작구'
        WHEN ROAD_NAME LIKE '%관악%' OR ST_NODE_NAME LIKE '%관악%' OR ED_NODE_NAME LIKE '%관악%' THEN '관악구'
        WHEN ROAD_NAME LIKE '%금천%' OR ST_NODE_NAME LIKE '%금천%' OR ED_NODE_NAME LIKE '%금천%' THEN '금천구'
        WHEN ROAD_NAME LIKE '%구로%' OR ST_NODE_NAME LIKE '%구로%' OR ED_NODE_NAME LIKE '%구로%' THEN '구로구'
        WHEN ROAD_NAME LIKE '%양천%' OR ST_NODE_NAME LIKE '%양천%' OR ED_NODE_NAME LIKE '%양천%' THEN '양천구'
        ELSE NULL
    END AS SGG,
    COUNT(*) AS row_cnt
FROM KOREA_TRAFFIC_SPEED__VOLUME_DATA__TMAP_NATIONWIDE_COVERAGE.TRAFFIC.TMAP_TRAFFIC_VOLUME
GROUP BY 1
HAVING SGG IS NOT NULL
ORDER BY 2 DESC;


-- ============================================================
-- STEP 1: 전처리 테이블 8개 생성
-- 기존 4개 (시세/전입인구 훈련/탐지) + 통신 개통 2개
-- ============================================================
USE DATABASE MOVERADAR;
USE SCHEMA PUBLIC;

-- 1-1. 시세 훈련 테이블 (2021~2023, 서울 전체)
CREATE OR REPLACE TABLE price_timeseries_train_view AS
SELECT
    YYYYMMDD::DATE                        AS TS,
    SGG || '_' || EMD                     AS REGION_KEY,
    SGG,
    AVG(MEME_PRICE_PER_SUPPLY_PYEONG)    AS MEME_PRICE_PER_M2
FROM KOREA_REAL_ESTATE_APARTMENT_MARKET_INTELLIGENCE.HACKATHON_2026.REGION_APT_RICHGO_MARKET_PRICE_M_H
WHERE REGION_LEVEL = 'emd'
  AND SD = '서울'
  AND YYYYMMDD::DATE < '2024-01-01'
GROUP BY 1, 2, 3;

-- 1-2. 시세 탐지 테이블 (2024~)
CREATE OR REPLACE TABLE price_timeseries_detect_view AS
SELECT
    YYYYMMDD::DATE                        AS TS,
    SGG || '_' || EMD                     AS REGION_KEY,
    SGG,
    AVG(MEME_PRICE_PER_SUPPLY_PYEONG)    AS MEME_PRICE_PER_M2
FROM KOREA_REAL_ESTATE_APARTMENT_MARKET_INTELLIGENCE.HACKATHON_2026.REGION_APT_RICHGO_MARKET_PRICE_M_H
WHERE REGION_LEVEL = 'emd'
  AND SD = '서울'
  AND YYYYMMDD::DATE >= '2023-01-01'
  -- ⚠️ 서울 25개 구 중 2024 데이터 없는 구는 2023 데이터로 탐지됨 (정상)
GROUP BY 1, 2, 3;

-- 1-3. 전입인구 훈련 테이블 (SGG 단위, 2021~2023)
CREATE OR REPLACE TABLE pop_timeseries_train_view AS
SELECT
    YYYYMMDD                               AS TS,
    SGG                                    AS REGION_KEY,
    SUM(POPULATION)                        AS POPULATION
FROM KOREA_REAL_ESTATE_APARTMENT_MARKET_INTELLIGENCE.HACKATHON_2026.REGION_POPULATION_MOVEMENT
WHERE MOVEMENT_TYPE = '전입'
  AND REGION_LEVEL = 'sgg'
  AND SD = '서울'
  AND YYYYMMDD < '2024-01-01'
GROUP BY 1, 2;

-- 1-4. 전입인구 탐지 테이블 (2024~)
CREATE OR REPLACE TABLE pop_timeseries_detect_view AS
SELECT
    YYYYMMDD                               AS TS,
    SGG                                    AS REGION_KEY,
    SUM(POPULATION)                        AS POPULATION
FROM KOREA_REAL_ESTATE_APARTMENT_MARKET_INTELLIGENCE.HACKATHON_2026.REGION_POPULATION_MOVEMENT
WHERE MOVEMENT_TYPE = '전입'
  AND REGION_LEVEL = 'sgg'
  AND SD = '서울'
  AND YYYYMMDD >= '2023-01-01'
  -- ⚠️ 22개 구는 2023 데이터로, 3개 구는 2023+2024 데이터로 탐지됨 (정상)
GROUP BY 1, 2;

-- 1-5. 통신 개통 훈련 테이블 (아정네트웍스 V01, SGG 단위, ~2023)
-- OPEN_COUNT: 개통 완료 = 실제 이사/입주 완료된 신규 계약
-- ⚠️ STEP 0-5 에서 확인한 실제 뷰명으로 교체 (V01 → V01_MONTHLY_REGIONAL_CONTRACT_STATS 등)
-- ⚠️ INSTALL_STATE 값이 '서울특별시' 맞는지 STEP 0-6 결과로 확인
CREATE OR REPLACE TABLE telecom_timeseries_train AS
SELECT
    YEAR_MONTH                             AS TS,
    INSTALL_CITY                           AS REGION_KEY,
    SUM(OPEN_COUNT)                        AS TELECOM_OPEN_COUNT
FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.TELECOM_INSIGHTS.V01_MONTHLY_REGIONAL_CONTRACT_STATS
WHERE INSTALL_STATE = '서울'
  AND YEAR_MONTH < '2024-01-01'
GROUP BY 1, 2;

-- 1-6. 통신 개통 탐지 테이블 (2024~)
CREATE OR REPLACE TABLE telecom_timeseries_detect AS
SELECT
    YEAR_MONTH                             AS TS,
    INSTALL_CITY                           AS REGION_KEY,
    SUM(OPEN_COUNT)                        AS TELECOM_OPEN_COUNT
FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.TELECOM_INSIGHTS.V01_MONTHLY_REGIONAL_CONTRACT_STATS
WHERE INSTALL_STATE = '서울'
  AND YEAR_MONTH >= '2023-01-01'
  -- ⚠️ 통신 데이터는 25개 구 모두 2023+ 존재 (V01 기준)
GROUP BY 1, 2;

-- 1-7. TMAP 교통량 스냅샷 (LLM 컨텍스트 전용 — 이상 탐지 불가)
-- ⚠️ MONTHS_AVAILABLE = 0 (2025-05 1개월 제공): 이상 탐지 대신 현재 구별 교통 상황 컨텍스트로 활용
CREATE OR REPLACE TABLE TMAP_SNAPSHOT AS
SELECT
    CASE
        WHEN ROAD_NAME LIKE '%강남%' OR ST_NODE_NAME LIKE '%강남%' OR ED_NODE_NAME LIKE '%강남%' THEN '강남구'
        WHEN ROAD_NAME LIKE '%서초%' OR ST_NODE_NAME LIKE '%서초%' OR ED_NODE_NAME LIKE '%서초%' THEN '서초구'
        WHEN ROAD_NAME LIKE '%영등포%' OR ST_NODE_NAME LIKE '%영등포%' OR ED_NODE_NAME LIKE '%영등포%' THEN '영등포구'
        WHEN ROAD_NAME LIKE '%마포%' OR ST_NODE_NAME LIKE '%마포%' OR ED_NODE_NAME LIKE '%마포%' THEN '마포구'
        WHEN ROAD_NAME LIKE '%종로%' OR ST_NODE_NAME LIKE '%종로%' OR ED_NODE_NAME LIKE '%종로%' THEN '종로구'
        WHEN ROAD_NAME LIKE '%중랑%' OR ST_NODE_NAME LIKE '%중랑%' OR ED_NODE_NAME LIKE '%중랑%' THEN '중랑구'
        WHEN ROAD_NAME LIKE '%중구%' OR ST_NODE_NAME LIKE '%중구%' OR ED_NODE_NAME LIKE '%중구%' THEN '중구'
        WHEN ROAD_NAME LIKE '%송파%' OR ST_NODE_NAME LIKE '%송파%' OR ED_NODE_NAME LIKE '%송파%' THEN '송파구'
        WHEN ROAD_NAME LIKE '%강동%' OR ST_NODE_NAME LIKE '%강동%' OR ED_NODE_NAME LIKE '%강동%' THEN '강동구'
        WHEN ROAD_NAME LIKE '%강서%' OR ST_NODE_NAME LIKE '%강서%' OR ED_NODE_NAME LIKE '%강서%' THEN '강서구'
        WHEN ROAD_NAME LIKE '%강북%' OR ST_NODE_NAME LIKE '%강북%' OR ED_NODE_NAME LIKE '%강북%' THEN '강북구'
        WHEN ROAD_NAME LIKE '%노원%' OR ST_NODE_NAME LIKE '%노원%' OR ED_NODE_NAME LIKE '%노원%' THEN '노원구'
        WHEN ROAD_NAME LIKE '%도봉%' OR ST_NODE_NAME LIKE '%도봉%' OR ED_NODE_NAME LIKE '%도봉%' THEN '도봉구'
        WHEN ROAD_NAME LIKE '%성북%' OR ST_NODE_NAME LIKE '%성북%' OR ED_NODE_NAME LIKE '%성북%' THEN '성북구'
        WHEN ROAD_NAME LIKE '%동대문%' OR ST_NODE_NAME LIKE '%동대문%' OR ED_NODE_NAME LIKE '%동대문%' THEN '동대문구'
        WHEN ROAD_NAME LIKE '%광진%' OR ST_NODE_NAME LIKE '%광진%' OR ED_NODE_NAME LIKE '%광진%' THEN '광진구'
        WHEN ROAD_NAME LIKE '%성동%' OR ST_NODE_NAME LIKE '%성동%' OR ED_NODE_NAME LIKE '%성동%' THEN '성동구'
        WHEN ROAD_NAME LIKE '%용산%' OR ST_NODE_NAME LIKE '%용산%' OR ED_NODE_NAME LIKE '%용산%' THEN '용산구'
        WHEN ROAD_NAME LIKE '%은평%' OR ST_NODE_NAME LIKE '%은평%' OR ED_NODE_NAME LIKE '%은평%' THEN '은평구'
        WHEN ROAD_NAME LIKE '%서대문%' OR ST_NODE_NAME LIKE '%서대문%' OR ED_NODE_NAME LIKE '%서대문%' THEN '서대문구'
        WHEN ROAD_NAME LIKE '%동작%' OR ST_NODE_NAME LIKE '%동작%' OR ED_NODE_NAME LIKE '%동작%' THEN '동작구'
        WHEN ROAD_NAME LIKE '%관악%' OR ST_NODE_NAME LIKE '%관악%' OR ED_NODE_NAME LIKE '%관악%' THEN '관악구'
        WHEN ROAD_NAME LIKE '%금천%' OR ST_NODE_NAME LIKE '%금천%' OR ED_NODE_NAME LIKE '%금천%' THEN '금천구'
        WHEN ROAD_NAME LIKE '%구로%' OR ST_NODE_NAME LIKE '%구로%' OR ED_NODE_NAME LIKE '%구로%' THEN '구로구'
        WHEN ROAD_NAME LIKE '%양천%' OR ST_NODE_NAME LIKE '%양천%' OR ED_NODE_NAME LIKE '%양천%' THEN '양천구'
        ELSE NULL
    END                              AS SGG,
    ROUND(AVG(AVG_SPEED), 1)         AS AVG_SPEED_KMPH,
    SUM(SUM_PROBE_COUNT)             AS TOTAL_PROBE_COUNT,
    COUNT(DISTINCT KSLINK_ID)        AS ROAD_LINK_COUNT
FROM KOREA_TRAFFIC_SPEED__VOLUME_DATA__TMAP_NATIONWIDE_COVERAGE.TRAFFIC.TMAP_TRAFFIC_VOLUME
GROUP BY 1
HAVING SGG IS NOT NULL;

SELECT * FROM TMAP_SNAPSHOT ORDER BY TOTAL_PROBE_COUNT DESC;


-- ============================================================
-- VALIDATION 1: 테이블 행수 및 REGION_KEY 확인
-- ============================================================

SELECT 'price_train'    AS tbl, COUNT(*) AS row_count, COUNT(DISTINCT REGION_KEY) AS regions FROM price_timeseries_train_view
UNION ALL
SELECT 'price_detect',   COUNT(*), COUNT(DISTINCT REGION_KEY) FROM price_timeseries_detect_view
UNION ALL
SELECT 'pop_train',      COUNT(*), COUNT(DISTINCT REGION_KEY) FROM pop_timeseries_train_view
UNION ALL
SELECT 'pop_detect',     COUNT(*), COUNT(DISTINCT REGION_KEY) FROM pop_timeseries_detect_view
UNION ALL
SELECT 'telecom_train',  COUNT(*), COUNT(DISTINCT REGION_KEY) FROM telecom_timeseries_train
UNION ALL
SELECT 'telecom_detect', COUNT(*), COUNT(DISTINCT REGION_KEY) FROM telecom_timeseries_detect
-- 기대: 모든 row_count > 0, telecom regions 최대 서울 25개 구

-- 통신 REGION_KEY 샘플 (SGG명과 일치하는지 확인)
SELECT DISTINCT REGION_KEY FROM telecom_timeseries_train ORDER BY 1 LIMIT 30;


-- ============================================================
-- STEP 2: 고객 프로파일 테이블 (서울 전체)
-- ============================================================
USE DATABASE MOVERADAR; USE SCHEMA PUBLIC;

CREATE OR REPLACE TABLE customer_profile_view AS
WITH district_stats AS (
    SELECT
        DISTRICT_CODE,
        SUM(CASE WHEN AGE_GROUP BETWEEN 20 AND 29 THEN CUSTOMER_COUNT ELSE 0 END) AS CNT_20S,
        SUM(CASE WHEN AGE_GROUP BETWEEN 30 AND 39 THEN CUSTOMER_COUNT ELSE 0 END) AS CNT_30S,
        SUM(CASE WHEN AGE_GROUP BETWEEN 40 AND 49 THEN CUSTOMER_COUNT ELSE 0 END) AS CNT_40S,
        SUM(CASE WHEN AGE_GROUP BETWEEN 50 AND 59 THEN CUSTOMER_COUNT ELSE 0 END) AS CNT_50S,
        AVG(RATE_INCOME_OVER_70M)                                                  AS RATE_INCOME_OVER_70M
    FROM SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.ASSET_INCOME_INFO
    WHERE STANDARD_YEAR_MONTH = (
        SELECT MAX(STANDARD_YEAR_MONTH)
        FROM SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.ASSET_INCOME_INFO
    )
    GROUP BY DISTRICT_CODE
)
SELECT
    m.DISTRICT_CODE,
    m.PROVINCE_CODE,
    m.CITY_KOR_NAME                                            AS SGG,
    m.DISTRICT_KOR_NAME                                        AS EMD,
    m.CITY_KOR_NAME || '_' || m.DISTRICT_KOR_NAME             AS REGION_KEY,
    m.DISTRICT_GEOM,
    CASE
        WHEN d.CNT_30S IS NULL THEN '30-40대 추정'
        WHEN d.CNT_30S = GREATEST(COALESCE(d.CNT_20S,0), d.CNT_30S, COALESCE(d.CNT_40S,0), COALESCE(d.CNT_50S,0)) THEN '30대 중심'
        WHEN d.CNT_40S = GREATEST(COALESCE(d.CNT_20S,0), COALESCE(d.CNT_30S,0), d.CNT_40S, COALESCE(d.CNT_50S,0)) THEN '40대 중심'
        WHEN d.CNT_20S = GREATEST(d.CNT_20S, COALESCE(d.CNT_30S,0), COALESCE(d.CNT_40S,0), COALESCE(d.CNT_50S,0)) THEN '20대 중심'
        ELSE '20-50대 혼합'
    END                                                        AS DOMINANT_AGE_GROUP,
    CASE
        WHEN d.RATE_INCOME_OVER_70M IS NULL THEN '소득 정보 없음'
        WHEN d.RATE_INCOME_OVER_70M > 0.2   THEN '고소득 지역'
        WHEN d.RATE_INCOME_OVER_70M > 0.1   THEN '중고소득 지역'
        ELSE '중소득 지역'
    END                                                        AS INCOME_PROFILE
FROM SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.M_SCCO_MST m
LEFT JOIN district_stats d ON m.DISTRICT_CODE = d.DISTRICT_CODE
WHERE m.DISTRICT_CODE IS NOT NULL;
-- ⚠️ WHERE 절에 시도 필터 없음 — M_SCCO_MST가 서울 전체를 커버하는지 STEP 0-4 결과로 확인
-- 만약 범위 초과 시: AND m.PROVINCE_CODE = '11' (서울 코드) 추가

SELECT COUNT(*) AS total_districts, COUNT(DISTINCT SGG) AS sgg_count FROM customer_profile_view;
SELECT * FROM customer_profile_view LIMIT 5;


-- ============================================================
-- STEP 3: Cortex Anomaly Detection 모델 학습 (3개)
-- ⚠️ 수 분 걸림 — 완료 확인 후 다음 진행
-- ============================================================
USE DATABASE MOVERADAR; USE SCHEMA PUBLIC;

-- 3-1. 시세 모델
CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION price_anomaly_model(
    INPUT_DATA        => SYSTEM$REFERENCE('TABLE', 'MOVERADAR.PUBLIC.price_timeseries_train_view'),
    SERIES_COLNAME    => 'REGION_KEY',
    TIMESTAMP_COLNAME => 'TS',
    TARGET_COLNAME    => 'MEME_PRICE_PER_M2',
    LABEL_COLNAME     => NULL
);

-- 3-2. 전입인구 모델
CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION pop_anomaly_model(
    INPUT_DATA        => SYSTEM$REFERENCE('TABLE', 'MOVERADAR.PUBLIC.pop_timeseries_train_view'),
    SERIES_COLNAME    => 'REGION_KEY',
    TIMESTAMP_COLNAME => 'TS',
    TARGET_COLNAME    => 'POPULATION',
    LABEL_COLNAME     => NULL
);

-- 3-3. 통신 개통 모델
CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION telecom_anomaly_model(
    INPUT_DATA        => SYSTEM$REFERENCE('TABLE', 'MOVERADAR.PUBLIC.telecom_timeseries_train'),
    SERIES_COLNAME    => 'REGION_KEY',
    TIMESTAMP_COLNAME => 'TS',
    TARGET_COLNAME    => 'TELECOM_OPEN_COUNT',
    LABEL_COLNAME     => NULL
);

-- 3-4. TMAP 모델 — 데이터 1개월(MONTHS_AVAILABLE=0)으로 학습 불가, 건너뜀


-- ============================================================
-- STEP 4: 이상 탐지 실행 (3개 모델)
-- ============================================================
USE DATABASE MOVERADAR; USE SCHEMA PUBLIC;

-- 4-1. 시세 이상 탐지
CREATE OR REPLACE TABLE PRICE_ANOMALY_RESULTS AS
SELECT * FROM TABLE(
    price_anomaly_model!DETECT_ANOMALIES(
        INPUT_DATA        => SYSTEM$REFERENCE('TABLE', 'MOVERADAR.PUBLIC.price_timeseries_detect_view'),
        SERIES_COLNAME    => 'REGION_KEY',
        TIMESTAMP_COLNAME => 'TS',
        TARGET_COLNAME    => 'MEME_PRICE_PER_M2'
    )
);

-- 4-2. 전입인구 이상 탐지
CREATE OR REPLACE TABLE POP_ANOMALY_RESULTS AS
SELECT * FROM TABLE(
    pop_anomaly_model!DETECT_ANOMALIES(
        INPUT_DATA        => SYSTEM$REFERENCE('TABLE', 'MOVERADAR.PUBLIC.pop_timeseries_detect_view'),
        SERIES_COLNAME    => 'REGION_KEY',
        TIMESTAMP_COLNAME => 'TS',
        TARGET_COLNAME    => 'POPULATION'
    )
);

-- 4-3. 통신 개통 이상 탐지
CREATE OR REPLACE TABLE TELECOM_ANOMALY_RESULTS AS
SELECT * FROM TABLE(
    telecom_anomaly_model!DETECT_ANOMALIES(
        INPUT_DATA        => SYSTEM$REFERENCE('TABLE', 'MOVERADAR.PUBLIC.telecom_timeseries_detect'),
        SERIES_COLNAME    => 'REGION_KEY',
        TIMESTAMP_COLNAME => 'TS',
        TARGET_COLNAME    => 'TELECOM_OPEN_COUNT'
    )
);

-- 4-4. TMAP 탐지 — 건너뜀 (데이터 부족)


-- ============================================================
-- VALIDATION 2: 탐지 결과 확인
-- ============================================================

SELECT
    'price'   AS signal, COUNT(*) AS total, SUM(CASE WHEN IS_ANOMALY THEN 1 ELSE 0 END) AS is_anomaly_true,
    MIN(PERCENTILE) AS min_pct, MAX(PERCENTILE) AS max_pct
FROM PRICE_ANOMALY_RESULTS
UNION ALL
SELECT 'pop', COUNT(*), SUM(CASE WHEN IS_ANOMALY THEN 1 ELSE 0 END), MIN(PERCENTILE), MAX(PERCENTILE)
FROM POP_ANOMALY_RESULTS
UNION ALL
SELECT 'telecom', COUNT(*), SUM(CASE WHEN IS_ANOMALY THEN 1 ELSE 0 END), MIN(PERCENTILE), MAX(PERCENTILE)
FROM TELECOM_ANOMALY_RESULTS
-- 기대: 모든 total > 0, PERCENTILE 범위 0~1


-- ============================================================
-- STEP 5: REGION_ALERTS — 3개 신호 통합
USE DATABASE MOVERADAR; USE SCHEMA PUBLIC;
--
-- 컬럼 매핑:
--   PRICE_ANOMALY_RESULTS.SERIES   = "서초구_방배동" (SGG_EMD)
--   POP_ANOMALY_RESULTS.SERIES     = "서초구" (SGG)
--   TELECOM_ANOMALY_RESULTS.SERIES = "서초구" (SGG, INSTALL_CITY)
--
-- COMBINED_SCORE = |price_pct - 0.5| × 2 × 0.50   (시세: 가장 강한 선행 신호)
--                + |pop_pct   - 0.5| × 2 × 0.30   (전입인구: 이사 직접 증거)
--                + |tc_pct    - 0.5| × 2 × 0.20   (통신개통: 실제 입주 증거)
--
-- 경보 임계값: PERCENTILE > 0.75 (급등) 또는 < 0.25 (급락)
-- ============================================================

CREATE OR REPLACE TABLE REGION_ALERTS AS
WITH base AS (
    SELECT
        p.SERIES                                                    AS REGION_KEY,
        SPLIT_PART(p.SERIES, '_', 1)                               AS SGG,
        SPLIT_PART(p.SERIES, '_', 2)                               AS EMD,
        p.TS                                                        AS ALERT_DATE,

        -- 시세 신호 (가중치 0.50)
        p.PERCENTILE                                                AS PRICE_PERCENTILE,
        p.Y                                                         AS PRICE_ACTUAL,
        p.FORECAST                                                  AS PRICE_FORECAST,
        (p.PERCENTILE > 0.75 OR p.PERCENTILE < 0.25)              AS PRICE_IS_ANOMALY,

        -- 전입인구 신호 (가중치 0.30)
        COALESCE(pop.PERCENTILE, 0.5)                              AS POP_PERCENTILE,
        pop.Y                                                       AS POP_ACTUAL,
        pop.FORECAST                                                AS POP_FORECAST,
        (COALESCE(pop.PERCENTILE, 0.5) > 0.75
         OR COALESCE(pop.PERCENTILE, 0.5) < 0.25)                 AS POP_IS_ANOMALY,

        -- 통신 개통 신호 (가중치 0.20)
        COALESCE(tc.PERCENTILE, 0.5)                               AS TELECOM_PERCENTILE,
        tc.Y                                                        AS TELECOM_ACTUAL,
        tc.FORECAST                                                 AS TELECOM_FORECAST,
        (COALESCE(tc.PERCENTILE, 0.5) > 0.75
         OR COALESCE(tc.PERCENTILE, 0.5) < 0.25)                  AS TELECOM_IS_ANOMALY,

        -- 결합 점수 (높을수록 이상도 강함, max=1.0)
        (ABS(p.PERCENTILE - 0.5) * 2 * 0.50
         + ABS(COALESCE(pop.PERCENTILE, 0.5) - 0.5) * 2 * 0.30
         + ABS(COALESCE(tc.PERCENTILE,  0.5) - 0.5) * 2 * 0.20) AS COMBINED_SCORE
    FROM PRICE_ANOMALY_RESULTS p
    LEFT JOIN POP_ANOMALY_RESULTS pop
        ON SPLIT_PART(p.SERIES, '_', 1) = pop.SERIES AND p.TS = pop.TS
    LEFT JOIN TELECOM_ANOMALY_RESULTS tc
        ON SPLIT_PART(p.SERIES, '_', 1) = tc.SERIES AND p.TS = tc.TS
    WHERE (p.PERCENTILE > 0.75 OR p.PERCENTILE < 0.25)
       OR (COALESCE(pop.PERCENTILE, 0.5) > 0.75 OR COALESCE(pop.PERCENTILE, 0.5) < 0.25)
       OR (COALESCE(tc.PERCENTILE,  0.5) > 0.75 OR COALESCE(tc.PERCENTILE,  0.5) < 0.25)
)
SELECT
    b.*,
    m.DISTRICT_CODE,
    m.DISTRICT_GEOM,
    cp.DOMINANT_AGE_GROUP,
    cp.INCOME_PROFILE,
    CASE
        WHEN b.PRICE_IS_ANOMALY AND b.POP_IS_ANOMALY AND b.TELECOM_IS_ANOMALY THEN '3중 동시 경보'
        WHEN b.PRICE_IS_ANOMALY AND b.POP_IS_ANOMALY                          THEN '시세+전입인구 경보'
        WHEN b.PRICE_IS_ANOMALY AND b.TELECOM_IS_ANOMALY                      THEN '시세+통신 경보'
        WHEN b.POP_IS_ANOMALY   AND b.TELECOM_IS_ANOMALY                      THEN '전입인구+통신 경보'
        WHEN b.PRICE_IS_ANOMALY                                                THEN '시세 경보'
        WHEN b.POP_IS_ANOMALY                                                  THEN '전입인구 경보'
        WHEN b.TELECOM_IS_ANOMALY                                              THEN '통신 경보'
    END                                                                        AS ALERT_TYPE
FROM base b
LEFT JOIN SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.M_SCCO_MST m
    ON (m.CITY_KOR_NAME || '_' || m.DISTRICT_KOR_NAME) = b.REGION_KEY
LEFT JOIN customer_profile_view cp
    ON b.REGION_KEY = cp.REGION_KEY;


-- ============================================================
-- VALIDATION 3: REGION_ALERTS 확인
-- ============================================================

SELECT COUNT(*) AS total, MIN(COMBINED_SCORE) AS min_score, MAX(COMBINED_SCORE) AS max_score
FROM REGION_ALERTS;

SELECT ALERT_TYPE, COUNT(*) AS cnt
FROM REGION_ALERTS GROUP BY 1 ORDER BY cnt DESC;

SELECT SGG, EMD, ALERT_TYPE, ROUND(COMBINED_SCORE, 3) AS score, ALERT_DATE
FROM REGION_ALERTS ORDER BY COMBINED_SCORE DESC LIMIT 15;


-- ============================================================
-- STEP 6a: 렌탈 트렌드 컨텍스트 (V06 → LLM 프롬프트용)
-- 최근 3개월 서울 기준 TOP 렌탈 상품 추출
-- ============================================================
USE DATABASE MOVERADAR; USE SCHEMA PUBLIC;

CREATE OR REPLACE TABLE RENTAL_TREND_CONTEXT AS
SELECT
    RENTAL_MAIN_CATEGORY,
    RENTAL_SUB_CATEGORY,
    SUM(OPEN_COUNT)                           AS TOTAL_OPEN,
    AVG(AVG_POLICY_AMOUNT)                    AS AVG_POLICY_AMT,
    ROUND(AVG(OPEN_CVR), 1)                   AS AVG_CVR_PCT,
    ROW_NUMBER() OVER (ORDER BY SUM(OPEN_COUNT) DESC) AS RANK_NUM
FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.TELECOM_INSIGHTS.V06_RENTAL_CATEGORY_TRENDS
WHERE YEAR_MONTH >= DATEADD('MONTH', -3, (SELECT MAX(YEAR_MONTH) FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.TELECOM_INSIGHTS.V06_RENTAL_CATEGORY_TRENDS))
  AND RENTAL_SUB_CATEGORY != '미분류'
  AND RENTAL_MAIN_CATEGORY != '미분류'
GROUP BY 1, 2;

-- TOP 5 렌탈 상품 확인
SELECT * FROM RENTAL_TREND_CONTEXT WHERE RANK_NUM <= 5 ORDER BY RANK_NUM;


-- ============================================================
-- STEP 6b: 콜센터 컨텍스트 (V09/V11 → 수요 강도 지표)
-- ============================================================

CREATE OR REPLACE TABLE CALL_CENTER_CONTEXT AS
SELECT
    v9.YEAR_MONTH,
    v9.MAIN_CATEGORY_NAME,
    SUM(v9.CALL_COUNT)                         AS TOTAL_CALLS,
    AVG(v9.CONNECTION_RATE)                    AS AVG_CONNECTION_RATE,
    AVG(v9.AVG_BILL_MINUTE)                    AS AVG_CALL_MINUTES,
    AVG(v11.CALL_TO_CONTRACT_CVR)              AS CALL_TO_CONTRACT_CVR,
    AVG(v11.AVG_LEADTIME_DAYS)                 AS AVG_LEADTIME_DAYS
FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.TELECOM_INSIGHTS.V09_MONTHLY_CALL_STATS v9
LEFT JOIN SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.TELECOM_INSIGHTS.V11_CALL_TO_CONTRACT_CONVERSION v11
    ON v9.YEAR_MONTH = v11.YEAR_MONTH
    AND v9.MAIN_CATEGORY_NAME = v11.MAIN_CATEGORY_NAME
    AND v9.DIVISION_NAME = v11.DIVISION_NAME
WHERE v9.DIVISION_NAME = '수신'
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;

SELECT * FROM CALL_CENTER_CONTEXT ORDER BY YEAR_MONTH DESC LIMIT 10;


-- ============================================================
-- STEP 6c: MARKETING_ALERTS — Cortex COMPLETE 마케팅 문구
-- 대상: COMBINED_SCORE > 0.25 (완화), SGG별 TOP 15 (확대)
-- LLM: messages array + temperature 0.9 (다양성 확보)
-- 콜센터 컨텍스트(CALL_CENTER_CONTEXT) 프롬프트 반영
-- 실제 percentile 수치 포함
-- ============================================================
USE DATABASE MOVERADAR; USE SCHEMA PUBLIC;

CREATE OR REPLACE TABLE MARKETING_ALERTS AS
WITH top_rental AS (
    SELECT LISTAGG(RENTAL_SUB_CATEGORY, '·') WITHIN GROUP (ORDER BY RANK_NUM) AS rental_products
    FROM RENTAL_TREND_CONTEXT
    WHERE RANK_NUM <= 3
),
-- TMAP 스냅샷: 구별 평균 속도 (낮을수록 교통 혼잡 = 이동 활발)
tmap_ctx AS (
    SELECT
        SGG,
        AVG_SPEED_KMPH,
        CASE
            WHEN AVG_SPEED_KMPH < 20 THEN '교통 혼잡'
            WHEN AVG_SPEED_KMPH < 35 THEN '교통 보통'
            ELSE '교통 원활'
        END AS TRAFFIC_STATUS
    FROM TMAP_SNAPSHOT
),
-- 콜센터: 최신 월 기준 상위 카테고리 연결률/전환율
latest_call AS (
    SELECT
        LISTAGG(MAIN_CATEGORY_NAME || '(' || ROUND(AVG_CONNECTION_RATE*100,0) || '%연결·전환' || ROUND(CALL_TO_CONTRACT_CVR*100,1) || '%)', '·')
            WITHIN GROUP (ORDER BY TOTAL_CALLS DESC) AS call_context
    FROM (
        SELECT * FROM CALL_CENTER_CONTEXT
        WHERE YEAR_MONTH = (SELECT MAX(YEAR_MONTH) FROM CALL_CENTER_CONTEXT)
          AND MAIN_CATEGORY_NAME != '기타'
        ORDER BY TOTAL_CALLS DESC
        LIMIT 3
    )
),
-- 1단계: 동별 최고 점수 1개만 (월 중복 제거)
dedup_by_emd AS (
    SELECT * FROM REGION_ALERTS
    WHERE COMBINED_SCORE > 0.25
    QUALIFY ROW_NUMBER() OVER (PARTITION BY REGION_KEY ORDER BY COMBINED_SCORE DESC) = 1
),
-- 2단계: 구별 TOP 15 (다양한 동 포함)
alert_candidates AS (
    SELECT * FROM dedup_by_emd
    QUALIFY ROW_NUMBER() OVER (PARTITION BY SGG ORDER BY COMBINED_SCORE DESC) <= 15
)
SELECT
    r.REGION_KEY,
    r.SGG,
    r.EMD,
    r.DISTRICT_CODE,
    r.DISTRICT_GEOM,
    r.ALERT_DATE,
    r.ALERT_TYPE,
    ROUND(r.COMBINED_SCORE, 3)              AS COMBINED_SCORE,
    ROUND(r.PRICE_PERCENTILE, 3)            AS PRICE_SCORE,
    ROUND(r.POP_PERCENTILE, 3)              AS POP_SCORE,
    ROUND(r.TELECOM_PERCENTILE, 3)          AS TELECOM_SCORE,
    r.DOMINANT_AGE_GROUP,
    r.INCOME_PROFILE,
    tr.rental_products                      AS TRENDING_RENTALS,
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        ARRAY_CONSTRUCT(
            OBJECT_CONSTRUCT('role', 'system', 'content',
                '당신은 통신·렌탈 서비스 전문 카피라이터입니다. 반드시 지역명을 포함하고, 40자 이내의 광고 문구 한 줄만 출력하세요. 부가 설명, 큰따옴표, 줄바꿈 없이 문구만 출력합니다.'
            ),
            OBJECT_CONSTRUCT('role', 'user', 'content',
                CASE
                    WHEN r.ALERT_TYPE = '3중 동시 경보' THEN
                        '아래 조건에 맞는 이사 고객용 광고 문구를 1줄로 작성하세요.

[지역] ' || r.SGG || ' ' || r.EMD || ' (' || LEFT(r.ALERT_DATE::VARCHAR, 7) || ')
[고객] ' || COALESCE(r.DOMINANT_AGE_GROUP, '30-40대') || ' / ' || COALESCE(r.INCOME_PROFILE, '중고소득') || '
[경보강도] 상위 ' || ROUND((1 - r.COMBINED_SCORE) * 100, 0) || '% 수준 (시세·전입인구·통신 3중 동시 이상)
[시장신호] 시세 ' || ROUND(r.PRICE_PERCENTILE*100,0) || '%ile / 전입인구 ' || ROUND(r.POP_PERCENTILE*100,0) || '%ile / 통신개통 ' || ROUND(r.TELECOM_PERCENTILE*100,0) || '%ile
[수요강도] ' || COALESCE(lc.call_context, '인터넷 문의 급증') || '
[교통상황] ' || COALESCE(tm.TRAFFIC_STATUS, '정보 없음') || '
[인기상품] ' || COALESCE(tr.rental_products, '정수기·비데·공기청정기') || ' 중 하나 반드시 언급
[서비스] 이달 개통 시 렌탈 0원 + 기가 인터넷 결합
[금지] 설명 문장, 큰따옴표, 부가 설명 금지. 문구만.'

                    WHEN r.ALERT_TYPE = '시세+전입인구 경보' THEN
                        '아래 조건에 맞는 이사 고객용 광고 문구를 1줄로 작성하세요.

[지역] ' || r.SGG || ' ' || r.EMD || ' (' || LEFT(r.ALERT_DATE::VARCHAR, 7) || ')
[고객] ' || COALESCE(r.DOMINANT_AGE_GROUP, '30-40대') || ' / ' || COALESCE(r.INCOME_PROFILE, '중고소득') || '
[경보강도] 시세 ' || ROUND(r.PRICE_PERCENTILE*100,0) || '%ile · 전입인구 ' || ROUND(r.POP_PERCENTILE*100,0) || '%ile 동시 급등
[수요강도] ' || COALESCE(lc.call_context, '인터넷 문의 증가') || '
[인기상품] ' || COALESCE(tr.rental_products, '정수기·비데·공기청정기') || ' 중 하나 반드시 언급
[서비스] 이사 당일 렌탈 설치 + 기가 인터넷 결합 할인
[금지] 설명 문장, 큰따옴표 금지. 문구만.'

                    WHEN r.ALERT_TYPE = '시세+통신 경보' THEN
                        '아래 조건에 맞는 이사 고객용 광고 문구를 1줄로 작성하세요.

[지역] ' || r.SGG || ' ' || r.EMD || ' (' || LEFT(r.ALERT_DATE::VARCHAR, 7) || ')
[고객] ' || COALESCE(r.DOMINANT_AGE_GROUP, '30-40대') || ' / ' || COALESCE(r.INCOME_PROFILE, '중고소득') || '
[경보강도] 시세 ' || ROUND(r.PRICE_PERCENTILE*100,0) || '%ile · 통신개통 ' || ROUND(r.TELECOM_PERCENTILE*100,0) || '%ile 동시 급등
[수요강도] ' || COALESCE(lc.call_context, '개통 문의 급증') || '
[서비스] 기가 인터넷 당일 개통 + 렌탈 결합 월정액 할인
[조건] 인터넷 개통 언급 필수
[금지] 설명 문장, 큰따옴표 금지. 문구만.'

                    WHEN r.ALERT_TYPE = '전입인구+통신 경보' THEN
                        '아래 조건에 맞는 이사 고객용 광고 문구를 1줄로 작성하세요.

[지역] ' || r.SGG || ' ' || r.EMD || ' (' || LEFT(r.ALERT_DATE::VARCHAR, 7) || ')
[고객] ' || COALESCE(r.DOMINANT_AGE_GROUP, '30-40대') || ' / ' || COALESCE(r.INCOME_PROFILE, '중소득') || '
[경보강도] 전입인구 ' || ROUND(r.POP_PERCENTILE*100,0) || '%ile · 통신개통 ' || ROUND(r.TELECOM_PERCENTILE*100,0) || '%ile 동시 급증
[수요강도] ' || COALESCE(lc.call_context, '렌탈 문의 증가') || '
[인기상품] ' || COALESCE(tr.rental_products, '정수기·비데·공기청정기') || ' 중 하나 반드시 언급
[서비스] 인터넷+렌탈 결합 월 최대 30% 할인
[금지] 설명 문장, 큰따옴표 금지. 문구만.'

                    WHEN r.ALERT_TYPE = '시세 경보' THEN
                        '아래 조건에 맞는 이사 고객용 광고 문구를 1줄로 작성하세요.

[지역] ' || r.SGG || ' ' || r.EMD || ' (' || LEFT(r.ALERT_DATE::VARCHAR, 7) || ')
[고객] ' || COALESCE(r.DOMINANT_AGE_GROUP, '30-40대') || ' / ' || COALESCE(r.INCOME_PROFILE, '중고소득') || '
[경보강도] 시세 상위 ' || ROUND(r.PRICE_PERCENTILE*100,0) || '%ile 급등 지역
[수요강도] ' || COALESCE(lc.call_context, '프리미엄 문의 증가') || '
[서비스] 기가 인터넷 24시간 내 설치 보장
[조건] 프리미엄·속도 강조 필수
[금지] 설명 문장, 큰따옴표 금지. 문구만.'

                    WHEN r.ALERT_TYPE = '전입인구 경보' THEN
                        '아래 조건에 맞는 이사 고객용 광고 문구를 1줄로 작성하세요.

[지역] ' || r.SGG || ' ' || r.EMD || ' (' || LEFT(r.ALERT_DATE::VARCHAR, 7) || ')
[고객] ' || COALESCE(r.DOMINANT_AGE_GROUP, '30-40대') || ' / ' || COALESCE(r.INCOME_PROFILE, '중소득') || '
[경보강도] 전입인구 상위 ' || ROUND(r.POP_PERCENTILE*100,0) || '%ile 급증 지역
[수요강도] ' || COALESCE(lc.call_context, '이사 관련 문의 증가') || '
[인기상품] ' || COALESCE(tr.rental_products, '정수기·비데·공기청정기') || ' 중 하나 반드시 언급
[서비스] 이사 고객 인터넷+렌탈 동시 특가
[조건] 새집 시작 감성 필수
[금지] 설명 문장, 큰따옴표 금지. 문구만.'

                    ELSE
                        '아래 조건에 맞는 이사 고객용 광고 문구를 1줄로 작성하세요.

[지역] ' || r.SGG || ' ' || r.EMD || ' (' || LEFT(r.ALERT_DATE::VARCHAR, 7) || ')
[고객] ' || COALESCE(r.DOMINANT_AGE_GROUP, '30-40대') || ' / ' || COALESCE(r.INCOME_PROFILE, '중소득') || '
[경보강도] 통신 개통 상위 ' || ROUND(r.TELECOM_PERCENTILE*100,0) || '%ile 급증
[수요강도] ' || COALESCE(lc.call_context, '개통 문의 급증') || '
[서비스] 기가 인터넷 당일 개통 + 렌탈 첫 달 무료
[조건] 개통 속도·첫 달 혜택 강조 필수
[금지] 설명 문장, 큰따옴표 금지. 문구만.'
                END
            )
        ),
        OBJECT_CONSTRUCT('temperature', 0.9)
    )['choices'][0]['messages']::VARCHAR    AS MARKETING_COPY
FROM alert_candidates r
CROSS JOIN top_rental tr
CROSS JOIN latest_call lc
LEFT JOIN tmap_ctx tm ON r.SGG = tm.SGG;

-- 결과 확인
SELECT ALERT_TYPE, COUNT(*) AS cnt FROM MARKETING_ALERTS GROUP BY 1 ORDER BY cnt DESC;

SELECT SGG, EMD, ALERT_TYPE, ROUND(COMBINED_SCORE, 2) AS score,
       DOMINANT_AGE_GROUP, TRENDING_RENTALS,
       LEFT(MARKETING_COPY, 80) AS copy_preview
FROM MARKETING_ALERTS
ORDER BY COMBINED_SCORE DESC;


-- ============================================================
-- STEP 7: 대시보드 지원 테이블
-- ============================================================
USE DATABASE MOVERADAR; USE SCHEMA PUBLIC;

-- 7-1. 신호 시계열 통합 뷰 (Streamlit 트렌드 차트용)
CREATE OR REPLACE TABLE SIGNAL_TIMESERIES AS
SELECT
    SERIES                             AS REGION_KEY,
    SPLIT_PART(SERIES, '_', 1)         AS SGG,
    SPLIT_PART(SERIES, '_', 2)         AS EMD,
    TS,
    'price'                            AS SIGNAL_TYPE,
    Y                                  AS ACTUAL_VALUE,
    FORECAST                           AS FORECAST_VALUE,
    PERCENTILE,
    IS_ANOMALY
FROM PRICE_ANOMALY_RESULTS
UNION ALL
SELECT
    SERIES, SERIES, NULL, TS,
    'pop',
    Y, FORECAST, PERCENTILE, IS_ANOMALY
FROM POP_ANOMALY_RESULTS
UNION ALL
SELECT
    SERIES, SERIES, NULL, TS,
    'telecom',
    Y, FORECAST, PERCENTILE, IS_ANOMALY
FROM TELECOM_ANOMALY_RESULTS

-- 7-2. SGG 요약 (대시보드 상단 KPI용)
CREATE OR REPLACE TABLE SGG_SUMMARY AS
SELECT
    SGG,
    COUNT(*)                                                    AS TOTAL_ALERTS,
    SUM(CASE WHEN ALERT_TYPE = '3중 동시 경보'    THEN 1 ELSE 0 END) AS TRIPLE_ALERTS,
    SUM(CASE WHEN ALERT_TYPE LIKE '%시세%'        THEN 1 ELSE 0 END) AS PRICE_ALERTS,
    SUM(CASE WHEN ALERT_TYPE LIKE '%전입인구%'    THEN 1 ELSE 0 END) AS POP_ALERTS,
    SUM(CASE WHEN ALERT_TYPE LIKE '%통신%'        THEN 1 ELSE 0 END) AS TELECOM_ALERTS,
    ROUND(MAX(COMBINED_SCORE), 3)                               AS MAX_SCORE,
    ROUND(AVG(COMBINED_SCORE), 3)                               AS AVG_SCORE
FROM REGION_ALERTS
GROUP BY 1;

-- 7-3. V01 월별 서울 지역 통신 트렌드 스냅샷 (Streamlit 직접 쿼리 대신 캐시)
CREATE OR REPLACE TABLE TELECOM_REGIONAL_TREND AS
SELECT
    YEAR_MONTH,
    INSTALL_CITY                           AS SGG,
    MAIN_CATEGORY_NAME,
    SUM(CONTRACT_COUNT)                    AS CONTRACTS,
    SUM(OPEN_COUNT)                        AS OPENS,
    ROUND(AVG(OPEN_CVR), 1)               AS AVG_CVR_PCT,
    SUM(TOTAL_NET_SALES)                   AS NET_SALES
FROM SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.TELECOM_INSIGHTS.V01_MONTHLY_REGIONAL_CONTRACT_STATS
WHERE INSTALL_STATE = '서울'
GROUP BY 1, 2, 3;

-- 확인
SELECT COUNT(*) AS row_count, COUNT(DISTINCT SGG) AS sgg_count FROM TELECOM_REGIONAL_TREND;
SELECT * FROM SGG_SUMMARY ORDER BY TOTAL_ALERTS DESC LIMIT 10;
