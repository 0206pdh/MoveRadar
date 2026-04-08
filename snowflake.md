# Snowflake 구현 상세

이 문서는 MoveRadar 프로젝트가 Snowflake 안에서 어떻게 구성되어 있는지, 실제 코드 기준으로 정리한 설명서다. 기준 파일은 [pipeline.sql](C:/Users/DGSO1/MoveRadar/pipeline.sql)과 [streamlit_app.py](C:/Users/DGSO1/MoveRadar/streamlit_app.py)다.

## 1. 전체 아키텍처

이 프로젝트는 Snowflake를 단순 저장소가 아니라 전체 실행 플랫폼으로 사용한다.

1. Snowflake Marketplace 데이터셋을 직접 조회한다.
2. SQL로 전처리 테이블을 만든다.
3. `SNOWFLAKE.ML.ANOMALY_DETECTION`으로 시계열 모델을 학습한다.
4. `!DETECT_ANOMALIES`로 이상 구간을 점수화한다.
5. 여러 신호를 합쳐 `REGION_ALERTS`를 만든다.
6. `SNOWFLAKE.CORTEX.COMPLETE`로 마케팅 문구를 생성한다.
7. Snowflake Native Streamlit UI에서 Snowpark 세션으로 결과를 바로 조회한다.

즉, 데이터 수집, 분석, ML, LLM, UI가 전부 Snowflake 내부 오브젝트와 실행 환경 위에서 이어진다.

## 2. 어떤 Snowflake 기능을 썼는가

### 2.1 데이터베이스와 스키마

[pipeline.sql](C:/Users/DGSO1/MoveRadar/pipeline.sql) 시작부에서 `MOVERADAR` 데이터베이스와 `PUBLIC` 스키마를 만들고 이후 산출물을 모두 여기에 적재한다.

- `CREATE DATABASE IF NOT EXISTS MOVERADAR`
- `CREATE SCHEMA IF NOT EXISTS PUBLIC`
- 학습용 테이블, 탐지 결과, 요약 테이블, LLM 결과 테이블을 전부 이 스키마에 생성

이 구조 덕분에 Marketplace 원천 데이터와 프로젝트 산출물을 분리했다.

### 2.2 Snowflake Marketplace 데이터 사용

`pipeline.sql`은 외부 ETL 없이 Snowflake Marketplace에 연결된 데이터셋을 바로 읽는다. 실제로 사용하는 데이터 소스는 다음과 같다.

- 부동산 데이터: `KOREA_REAL_ESTATE_APARTMENT_MARKET_INTELLIGENCE.HACKATHON_2026`
- 자산/소득/소비 데이터: `SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA`
- 통신/렌탈/콜센터 데이터: `SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION.TELECOM_INSIGHTS`
- 교통 데이터: `KOREA_TRAFFIC_SPEED__VOLUME_DATA__TMAP_NATIONWIDE_COVERAGE.TRAFFIC`

사용 테이블과 뷰는 다음과 같다.

- 아파트 시세: `REGION_APT_RICHGO_MARKET_PRICE_M_H`
- 전입 인구: `REGION_POPULATION_MOVEMENT`
- 소득/자산: `ASSET_INCOME_INFO`
- 행정 매핑/지오메트리: `M_SCCO_MST`
- 통신 개통: `V01_MONTHLY_REGIONAL_CONTRACT_STATS`
- 렌탈 트렌드: `V06_RENTAL_CATEGORY_TRENDS`
- 콜센터 문의: `V09_MONTHLY_CALL_STATS`
- 콜->계약 전환: `V11_CALL_TO_CONTRACT_CONVERSION`
- 교통량/속도: `TMAP_TRAFFIC_VOLUME`

핵심은 파일 업로드형 수집이 아니라 Snowflake 안에서 이미 구독된 데이터 제품을 조합한 것이다.

### 2.3 SQL 기반 전처리와 피처 엔지니어링

[pipeline.sql](C:/Users/DGSO1/MoveRadar/pipeline.sql)에서는 `CREATE OR REPLACE TABLE ... AS SELECT` 형태로 분석용 테이블을 단계적으로 만든다.

대표 예시는 다음과 같다.

- 가격 학습/탐지 분리: `price_train_22sgg`, `price_train_3sgg`, `price_detect_22sgg`, `price_detect_3sgg`
- 인구 이동 학습/탐지: `pop_timeseries_train_view`, `pop_timeseries_detect_view`
- 통신 개통 학습/탐지: `telecom_timeseries_train`, `telecom_timeseries_detect`
- 카드 소비 학습/탐지: `card_timeseries_train`, `card_timeseries_detect`
- 고객 프로파일: `customer_profile_view`

여기서 한 일은 단순 집계가 아니라 모델이 바로 먹을 수 있는 시계열 데이터 구조로 바꾸는 것이다.

- `REGION_KEY`를 만들어 시계열 단위를 고정
- `TS`를 모델 입력용 타임스탬프로 통일
- `AVG`, `SUM`, `JOIN`, `CASE`, `ROW_NUMBER`, `LISTAGG`, `FIRST_VALUE`, `QUALIFY` 등 Snowflake SQL 기능으로 분석용 피처 생성
- 행정구역 코드와 지오메트리까지 함께 붙여 나중에 UI에서 바로 사용 가능하게 구성

## 3. 데이터는 어떻게 모으고 분석했는가

### 3.1 가격 데이터

[pipeline.sql](C:/Users/DGSO1/MoveRadar/pipeline.sql)에서는 아파트 실거래 시세를 EMD 단위로 집계해서 월별 신호로 만든다. `AVG(MEME_PRICE_PER_SUPPLY_PYEONG)`을 사용해 지역별 평균 가격 시계열을 구성한다.

특이한 점은 가격 모델을 두 개로 나눠 학습했다는 점이다.

- 22개 구는 `2023-01-01` 이전을 학습, 이후를 탐지
- 3개 구는 `2024-01-01` 이전을 학습, 이후를 탐지

이렇게 나눈 이유는 데이터 가용 기간이 구마다 다르고, Cortex Anomaly Detection이 탐지 구간이 학습 마지막 시점 이후여야 한다는 제약을 가지기 때문이다.

### 3.2 전입 인구 데이터

`REGION_POPULATION_MOVEMENT`에서 `MOVEMENT_TYPE = '전입'`, `REGION_LEVEL = 'sgg'`, `SD = '서울'` 조건을 걸어 서울 구 단위 전입 인구 시계열을 만든다. 이 값은 실제 이사 수요와 직접 연관된 선행 신호로 사용된다.

### 3.3 통신 개통 데이터

`V01_MONTHLY_REGIONAL_CONTRACT_STATS`에서 `OPEN_COUNT`를 구 단위로 집계한다. 새 주소지에서 통신 개통이 발생하면 입주 완료 또는 입주 직전 가능성이 높기 때문에, 이 프로젝트에서는 전입 인구보다 더 행동 기반인 후행 검증 신호로 본다.

같은 뷰에서 이후 `TELECOM_REGIONAL_TREND`도 따로 만들어 UI 탭에서 통신 카테고리별 추이 차트에 사용한다.

### 3.4 카드 소비 데이터

`CARD_SALES_INFO`와 `M_SCCO_MST`를 조인한 뒤, 이사 준비와 관련성이 높은 항목만 합산한다.

- `ELECTRONICS_FURNITURE_SALES`
- `HOME_LIFE_SERVICE_SALES`
- `LARGE_DISCOUNT_STORE_SALES`

즉, 단순 총소비가 아니라 가전/가구/생활서비스/대형마트 소비를 묶어 "이사 준비 소비"라는 별도 신호로 재정의했다.

### 3.5 고객 프로파일 데이터

`ASSET_INCOME_INFO`와 `M_SCCO_MST`를 이용해 `customer_profile_view`를 만든다. 여기서 Snowflake SQL로 만든 값은 다음과 같다.

- 우세 연령대: 20대, 30대, 40대, 50대 중 최대 고객군 기준
- 소득 프로파일: `RATE_INCOME_OVER_70M` 기준으로 고소득/중고소득/중소득 분류
- `DISTRICT_GEOM`: UI에서 지역 맥락 설명에 쓰는 공간 컬럼

이 정보는 이상 탐지 모델 학습에는 직접 넣지 않고, LLM 프롬프트에 고객 맥락으로 주입한다.

### 3.6 교통 데이터

`TMAP_TRAFFIC_VOLUME`은 `TMAP_SNAPSHOT` 테이블로 가공해 구별 평균 속도와 프로브 수를 만든다. 여기서는 `CASE WHEN ROAD_NAME LIKE ...` 패턴으로 도로명과 노드명에서 서울 구를 매핑한다.

중요한 판단은 이 데이터를 이상 탐지 모델에는 넣지 않았다는 점이다.

- 코드 주석 기준으로 데이터가 사실상 1개월 수준이라 장기 시계열 학습에 부적합
- 대신 `AVG_SPEED_KMPH`를 바탕으로 `교통 혼잡/보통/원활` 상태를 만들고 LLM 컨텍스트에만 사용

즉, 데이터 품질과 길이에 따라 "학습용 신호"와 "문맥 보강용 신호"를 구분해서 썼다.

## 4. Snowflake ML로 학습한 방식

### 4.1 사용 기능

[pipeline.sql](C:/Users/DGSO1/MoveRadar/pipeline.sql)의 STEP 3에서 `SNOWFLAKE.ML.ANOMALY_DETECTION`을 사용한다.

생성된 모델은 5개다.

- `price_anomaly_model_22`
- `price_anomaly_model_3`
- `pop_anomaly_model`
- `telecom_anomaly_model`
- `card_anomaly_model`

### 4.2 모델 생성 방식

모델 생성 시 공통적으로 다음 파라미터 패턴을 쓴다.

- `INPUT_DATA => SYSTEM$REFERENCE('TABLE', ...)`
- `SERIES_COLNAME => 'REGION_KEY'`
- `TIMESTAMP_COLNAME => 'TS'`
- `TARGET_COLNAME => 실제 수치 컬럼`
- `LABEL_COLNAME => NULL`

여기서 `SYSTEM$REFERENCE('TABLE', ...)`는 Snowflake 오브젝트를 모델 학습 입력으로 참조하는 방식이다. 별도 추출 파일 없이 테이블을 직접 연결하기 때문에 Snowflake 내부 파이프라인 성격이 강하다.

### 4.3 학습 철학

이 프로젝트의 학습은 "미래 예측값을 맞히는 범용 회귀"가 아니라 "정상 패턴에서 벗어난 급등/급락을 찾는 이상 탐지"에 맞춰져 있다. 그래서 각 신호별로 긴 시계열을 학습 구간으로 두고, 그 이후 구간에만 탐지를 수행한다.

이 접근의 장점은 다음과 같다.

- 절대값이 아니라 지역별 자기 이력 대비 이상 여부를 판단 가능
- 서로 단위가 다른 신호를 `PERCENTILE` 기반으로 비교 가능
- 가격, 전입, 통신, 카드 소비처럼 성격이 다른 데이터를 하나의 경보 체계로 결합 가능

## 5. 이상 탐지는 어떻게 돌렸는가

STEP 4에서 각 모델에 대해 `model_name!DETECT_ANOMALIES(...)`를 호출한다.

- 가격: 두 모델 결과를 `UNION ALL`로 합침
- 인구/통신/카드: 각각 탐지 결과 테이블 생성

결과 테이블은 다음 정보를 포함한다.

- `SERIES`
- `TS`
- `Y`
- `FORECAST`
- `PERCENTILE`
- `IS_ANOMALY`

이 프로젝트에서 특히 중요한 값은 `PERCENTILE`이다. 이후 결합 점수 계산과 경보 분류가 모두 이 상대 위치 값을 기준으로 만들어진다.

## 6. 여러 신호를 하나의 경보로 합치는 방법

[pipeline.sql](C:/Users/DGSO1/MoveRadar/pipeline.sql)의 `REGION_ALERTS`가 핵심 분석 테이블이다.

여기서 한 일은 다음과 같다.

1. 가격은 EMD 단위 결과를 다시 SGG 단위로 집계
2. 인구, 통신, 카드 신호를 날짜 기준으로 조인
3. 각 신호의 `PERCENTILE`을 사용해 점수 계산
4. 이상 여부 조합에 따라 `ALERT_TYPE`을 분류
5. 고객 프로파일과 지오메트리를 붙임

결합 점수 가중치는 코드 주석 기준으로 다음과 같다.

- 가격 0.40
- 전입 인구 0.25
- 통신 개통 0.20
- 카드 소비 0.15

점수식은 각 신호의 `ABS(PERCENTILE - 0.5) * 2`를 가중합하는 방식이다. 즉, 0.5에서 멀수록 평소 패턴에서 더 벗어난 것으로 본다.

또한 임계값도 코드에 명시돼 있다.

- `PERCENTILE > 0.75` 또는 `< 0.25`이면 강한 이상 신호로 간주

이 단계가 중요한 이유는 Snowflake ML의 원시 출력값을 실제 비즈니스 경보 체계로 바꾸는 부분이기 때문이다.

## 7. LLM은 어떻게 설정했는가

### 7.1 사용 기능

[pipeline.sql](C:/Users/DGSO1/MoveRadar/pipeline.sql) STEP 6c에서 `SNOWFLAKE.CORTEX.COMPLETE`를 사용한다.

모델은 코드에 명시적으로 다음과 같이 설정돼 있다.

- `mistral-large2`

호출 형태는 다음과 같다.

- 시스템 메시지 1개
- 사용자 메시지 1개
- `OBJECT_CONSTRUCT('temperature', 0.9)` 옵션 지정

즉, 단순 한 줄 프롬프트가 아니라 Snowflake Cortex의 messages 배열 형식을 사용한 채팅형 호출이다.

### 7.2 프롬프트 구조

LLM 입력에는 단순 경보 텍스트만 넣지 않았다. `MARKETING_ALERTS` 생성 로직을 보면 아래 컨텍스트를 조합한다.

- 지역명과 월
- 우세 연령대
- 소득 프로파일
- 가격/인구/통신/카드 퍼센타일
- 렌탈 인기 상품
- 최신 콜센터 수요 강도
- TMAP 기반 교통 상태
- 경보 유형별 맞춤 지시문

즉, LLM은 "이상 탐지 결과를 말로 요약"하는 수준이 아니라, 분석 결과 + 고객 맥락 + 운영 맥락을 합친 카피 생성기로 설계돼 있다.

### 7.3 컨텍스트용 테이블

LLM 보강을 위해 별도 컨텍스트 테이블도 만든다.

- `RENTAL_TREND_CONTEXT`: 최근 3개월 렌탈 카테고리 TOP 랭킹
- `CALL_CENTER_CONTEXT`: 월별 문의량, 연결률, 전환율, 리드타임
- `TMAP_SNAPSHOT`: 교통 혼잡도 판단용

그리고 `MARKETING_ALERTS` 생성 직전 다음과 같은 Snowflake SQL 기법을 쓴다.

- `LISTAGG`로 인기 상품 문자열 결합
- `ROW_NUMBER`와 `QUALIFY`로 지역별 상위 후보 선별
- `CASE`로 경보 유형별 다른 프롬프트 템플릿 적용
- `CROSS JOIN`으로 공통 컨텍스트 결합

이 설계 덕분에 LLM이 지역별로 더 구체적인 문구를 만들 수 있다.

## 8. UI는 Snowflake에서 어떻게 만들었는가

### 8.1 실행 방식

[streamlit_app.py](C:/Users/DGSO1/MoveRadar/streamlit_app.py) 첫 부분에서 `from snowflake.snowpark.context import get_active_session`을 사용하고, `session = get_active_session()`으로 Snowflake 세션을 받아온다.

이건 일반 외부 Streamlit 앱처럼 별도 DB 커넥션 문자열을 넣는 방식이 아니라, Snowflake Native Streamlit 환경에서 현재 활성 세션을 그대로 재사용하는 구조다.

### 8.2 Snowpark 사용 방식

UI의 데이터 로딩 함수들은 전부 `session.sql(...).to_pandas()` 패턴을 쓴다.

- `load_alerts()`
- `load_sgg_summary()`
- `load_signal_ts(region_key)`
- `load_telecom_trend(sgg)`
- `load_rental_context()`
- `load_call_center()`

즉, Python 안에서 직접 Pandas로 계산한 것이 아니라 Snowflake SQL 결과를 Snowpark 세션으로 가져와 시각화한다.

### 8.3 화면 구성

[streamlit_app.py](C:/Users/DGSO1/MoveRadar/streamlit_app.py)에는 4개 탭이 있다.

- `🔔 경보 현황`
- `📈 지역 신호 추이`
- `📱 통신 분석`
- `📣 마케팅 문구`

각 탭이 쓰는 Snowflake 산출물은 다음과 같다.

- 경보 현황: `MARKETING_ALERTS`
- 지역 신호 추이: `SIGNAL_TIMESERIES`, `SGG_SUMMARY`
- 통신 분석: `RENTAL_TREND_CONTEXT`, `CALL_CENTER_CONTEXT`, `TELECOM_REGIONAL_TREND`
- 마케팅 문구: `MARKETING_ALERTS`

즉, UI도 별도 백엔드 API 없이 Snowflake 테이블을 직접 읽는 구조다.

### 8.4 시각화 방식

UI는 Streamlit과 Altair를 사용한다.

- `st.metric`으로 KPI 카드
- `st.selectbox`, `st.slider`, `st.tabs`로 인터랙션
- `alt.Chart`로 막대/라인/히트맵/박스플롯 차트

특히 지역 신호 추이 탭은 `ACTUAL_VALUE`, `FORECAST_VALUE`, `PERCENTILE`, `IS_ANOMALY`를 같이 보여준다. 이 덕분에 Cortex ML이 무엇을 이상으로 판단했는지 시각적으로 해석할 수 있다.

## 9. 이 프로젝트에서 Snowflake를 잘 쓴 포인트

### 9.1 한 플랫폼 안에서 끝낸 점

보통은 데이터 웨어하우스, 모델링, LLM, 대시보드가 분리되는데, 이 프로젝트는 다음을 한곳에서 처리한다.

- Marketplace 데이터 접근
- SQL 전처리
- Cortex ML 이상 탐지
- Cortex COMPLETE 문구 생성
- Snowpark 세션 기반 Streamlit UI

이게 Snowflake 활용도의 핵심이다.

### 9.2 데이터 상태에 맞게 기능을 구분한 점

모든 데이터를 무리하게 모델 학습에 넣지 않았다.

- 장기 시계열이 있는 가격/전입/통신/카드 데이터는 ML 학습
- 시계열 길이가 짧은 TMAP은 LLM 컨텍스트 전용
- 고객 프로파일은 모델 입력이 아니라 프롬프트 보강용

즉, 기능을 보여주기 위한 억지 결합이 아니라 데이터 특성에 따라 역할을 다르게 배치했다.

### 9.3 Snowflake 오브젝트 설계가 명확한 점

테이블 이름만 봐도 단계가 분명하다.

- 원천 가공: `*_train`, `*_detect`
- ML 결과: `*_ANOMALY_RESULTS`
- 비즈니스 통합: `REGION_ALERTS`
- LLM 결과: `MARKETING_ALERTS`
- UI 캐시/요약: `SIGNAL_TIMESERIES`, `SGG_SUMMARY`, `TELECOM_REGIONAL_TREND`

이 구조는 발표나 데모에서 설명하기도 쉽다.

## 10. 파일별 근거

- Snowflake 파이프라인 전체: [pipeline.sql](C:/Users/DGSO1/MoveRadar/pipeline.sql)
- Native Streamlit + Snowpark UI: [streamlit_app.py](C:/Users/DGSO1/MoveRadar/streamlit_app.py)

핵심 코드 위치는 아래다.

- ML 모델 생성: [pipeline.sql](C:/Users/DGSO1/MoveRadar/pipeline.sql#L413)
- 이상 탐지 실행: [pipeline.sql](C:/Users/DGSO1/MoveRadar/pipeline.sql#L466)
- 통합 경보 생성: [pipeline.sql](C:/Users/DGSO1/MoveRadar/pipeline.sql#L559)
- 렌탈 컨텍스트: [pipeline.sql](C:/Users/DGSO1/MoveRadar/pipeline.sql#L693)
- 콜센터 컨텍스트: [pipeline.sql](C:/Users/DGSO1/MoveRadar/pipeline.sql#L715)
- Cortex COMPLETE 호출: [pipeline.sql](C:/Users/DGSO1/MoveRadar/pipeline.sql#L803)
- Streamlit 세션 연결: [streamlit_app.py](C:/Users/DGSO1/MoveRadar/streamlit_app.py#L6)
- Streamlit 탭 구성: [streamlit_app.py](C:/Users/DGSO1/MoveRadar/streamlit_app.py#L125)

## 11. 한 줄 정리

MoveRadar는 Snowflake Marketplace 데이터 위에서 SQL 전처리, Cortex ML 이상 탐지, Cortex COMPLETE 기반 마케팅 카피 생성, Snowpark 기반 Streamlit UI까지 연결한 Snowflake end-to-end 애플리케이션이다.
