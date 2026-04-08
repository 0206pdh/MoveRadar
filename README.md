# MoveRadar v2

이사 수요를 **1~2개월 선행 탐지**하는 Snowflake Native 인텔리전스 파이프라인.  
Snowflake KR Hackathon 2026 — 테크 트랙 출품작.

---

## 문제 정의

기존 마케팅 자동화는 CRM 참여 신호 기반 — 이미 관심을 표현한 사람에게만 반응한다.  
MoveRadar는 아직 이사를 인지하지 못한 사람을 **1~2개월 전에 구조적 시장 신호로 탐지**한다.

| 기존 방식 | MoveRadar |
|-----------|-----------|
| CRM 클릭/조회 반응 | 시장 신호 선제 탐지 |
| 이미 관심 있는 사람 | 아직 이사 모르는 사람 |
| 캠페인 단위 실행 | 동 단위 실시간 경보 |

---

## 아키텍처

```
[Snowflake Marketplace — 4개 데이터셋]
  Dataknows     : 아파트 시세 + 전입인구 (서울 전역 EMD/SGG)
  SPH           : 자산/소득 + 행정구역 폴리곤 (M_SCCO_MST)
  아정네트웍스  : 통신 개통 + 렌탈 트렌드 + 콜센터 (V01/V06/V09/V11)
  TMAP          : 전국 도로 교통량 (2,400만 사용자 실시간 프로브)
          |
          v
[STEP 1: 전처리 — 8개 훈련/탐지 테이블]
  price_timeseries_train/detect    ← EMD 단위 월별 시세
  pop_timeseries_train/detect      ← SGG 단위 월별 전입인구
  telecom_timeseries_train/detect  ← SGG 단위 월별 통신 개통
  card_timeseries_train/detect     ← SGG 단위 월별 카드소비 이사지표
          |
          v
[STEP 2: 고객 프로파일]
  customer_profile_view  ← 연령대 + 소득 구간 (M_SCCO_MST + ASSET_INCOME_INFO)
          |
          v
[STEP 3: Cortex Anomaly Detection 학습 — 4개 모델]
  price_anomaly_model    훈련: 2021~2023 (서울 전역 EMD)
  pop_anomaly_model      훈련: 2021~2022 (서울 25개 구)
  telecom_anomaly_model  훈련: 2023      (서울 25개 구)
  card_anomaly_model     훈련: ~2023     (서울 25개 구)
          |
          v
[STEP 4: 이상 탐지 실행 — 4개 결과 테이블]
  PRICE_ANOMALY_RESULTS    탐지: 2023~ (22개 구 2023, 3개 구 2024+)
  POP_ANOMALY_RESULTS      탐지: 2023~
  TELECOM_ANOMALY_RESULTS  탐지: 2024~
  CARD_ANOMALY_RESULTS     탐지: 2024~
          |
          v
[STEP 5: REGION_ALERTS — 4신호 통합]
  COMBINED_SCORE = |price_pct−0.5|×2×0.40
                 + |pop_pct−0.5|×2×0.25
                 + |tc_pct−0.5|×2×0.20
                 + |card_pct−0.5|×2×0.15
          |
          v
[STEP 6: Cortex COMPLETE — 마케팅 문구 생성]
  컨텍스트: 렌탈 트렌드(V06) + 콜센터 수요강도(V09/V11) + 실제 percentile 수치 (4신호)
  모델: mistral-large2 / temperature 0.9
  출력: 경보 유형별 맞춤 광고 문구 (40자 이내, 15가지 경보 유형)
          |
          v
[STEP 7: 대시보드 지원 테이블]
  SIGNAL_TIMESERIES    ← 4신호 시계열 통합 (Streamlit 트렌드 차트)
  SGG_SUMMARY          ← 구별 경보 현황 KPI
  TELECOM_REGIONAL_TREND ← 통신 트렌드 스냅샷
          |
          v
[Streamlit Native App — 4탭 대시보드]
  탭1: 지도 (경보 지역 + 신호 강도 히트맵)
  탭2: 트렌드 (4신호 시계열 + 이상 포인트)
  탭3: 마케팅 문구 (경보별 LLM 생성 카피)
  탭4: 렌탈/통신 트렌드
```

---

## 신호 설계

| 신호 | 데이터 소스 | 의미 | 가중치 |
|------|------------|------|--------|
| 시세 | REGION_APT_RICHGO_MARKET_PRICE_M_H | 아파트 거래가 이상 = 수요 급변 | 0.40 |
| 전입인구 | REGION_POPULATION_MOVEMENT | 실제 이사 인구 급증 | 0.25 |
| 통신 개통 | V01_MONTHLY_REGIONAL_CONTRACT_STATS | 인터넷 신규 개통 = 입주 완료 증거 | 0.20 |
| 카드소비 | CARD_SALES_INFO | 가전·가구·할인점 소비 급증 = 이사 준비 선행 신호 | 0.15 |

**경보 임계값**: PERCENTILE > 0.75 (급등) 또는 < 0.25 (급락)

**훈련/탐지 분리 전략**:
- 시세: 훈련 ~2023, 탐지 2023~ (22개 구 2023 탐지, 3개 구 2024+ 추가 탐지)
- 전입인구: 훈련 2021~2022, 탐지 2023~ (no overlap)
- 통신 개통: 훈련 2023, 탐지 2024~ (V01 데이터 2023 시작, no overlap)
- 카드소비: 훈련 ~2023, 탐지 2024~ (CARD_SALES_INFO 시계열, no overlap)
- TMAP: 1개월치 데이터만 제공 → Anomaly Detection 대신 LLM 컨텍스트로만 활용 (TMAP_SNAPSHOT)

---

## 데이터 소스

| 제공사 | 테이블 | 용도 |
|--------|--------|------|
| Dataknows | REGION_APT_RICHGO_MARKET_PRICE_M_H | 아파트 시세 시계열 |
| Dataknows | REGION_POPULATION_MOVEMENT | 전입인구 시계열 |
| SPH | M_SCCO_MST | 행정구역 마스터 + 폴리곤 |
| SPH | ASSET_INCOME_INFO | 소득 구간 프로파일 |
| SPH | CARD_SALES_INFO | 카드소비 이사지표 |
| 아정네트웍스 | V01_MONTHLY_REGIONAL_CONTRACT_STATS | 통신 개통 시계열 |
| 아정네트웍스 | V06_RENTAL_CATEGORY_TRENDS | 렌탈 트렌드 컨텍스트 |
| 아정네트웍스 | V09_MONTHLY_CALL_STATS | 콜센터 수요 강도 |
| 아정네트웍스 | V11_CALL_TO_CONTRACT_CONVERSION | 콜→계약 전환율 |
| TMAP | TMAP_TRAFFIC_VOLUME | 교통 속도 컨텍스트 |

---

## 생성 테이블 목록 (MOVERADAR.PUBLIC)

| 테이블 | 설명 |
|--------|------|
| PRICE_TIMESERIES_TRAIN_VIEW | 시세 훈련 데이터 |
| PRICE_TIMESERIES_DETECT_VIEW | 시세 탐지 데이터 |
| POP_TIMESERIES_TRAIN_VIEW | 전입인구 훈련 데이터 |
| POP_TIMESERIES_DETECT_VIEW | 전입인구 탐지 데이터 |
| TELECOM_TIMESERIES_TRAIN | 통신 개통 훈련 데이터 |
| TELECOM_TIMESERIES_DETECT | 통신 개통 탐지 데이터 |
| CARD_TIMESERIES_TRAIN | 카드소비 이사지표 훈련 데이터 |
| CARD_TIMESERIES_DETECT | 카드소비 이사지표 탐지 데이터 |
| CUSTOMER_PROFILE_VIEW | 동별 연령대·소득 프로파일 |
| PRICE_ANOMALY_RESULTS | 시세 이상 탐지 결과 |
| POP_ANOMALY_RESULTS | 전입인구 이상 탐지 결과 |
| TELECOM_ANOMALY_RESULTS | 통신 개통 이상 탐지 결과 |
| CARD_ANOMALY_RESULTS | 카드소비 이상 탐지 결과 |
| REGION_ALERTS | 4신호 통합 경보 (COMBINED_SCORE) |
| RENTAL_TREND_CONTEXT | 렌탈 트렌드 (LLM 컨텍스트) |
| CALL_CENTER_CONTEXT | 콜센터 수요 강도 (LLM 컨텍스트) |
| MARKETING_ALERTS | LLM 생성 마케팅 문구 |
| SIGNAL_TIMESERIES | 4신호 시계열 통합 (대시보드용) |
| SGG_SUMMARY | 구별 경보 현황 KPI |
| TELECOM_REGIONAL_TREND | 통신 지역별 트렌드 스냅샷 |

---

## Cortex ML 모델

```sql
-- 4개 Anomaly Detection 모델 (MOVERADAR.PUBLIC)
price_anomaly_model
pop_anomaly_model
telecom_anomaly_model
card_anomaly_model
```

---

## LLM 프롬프트 전략

- **모델**: `mistral-large2` (Snowflake Cortex COMPLETE)
- **포맷**: messages array (system + user role 분리)
- **경보 유형별 맞춤 프롬프트**: 15가지 경보 유형 × 각각 다른 강조 포인트
- **컨텍스트 주입**:
  - `[시장신호]`: 실제 percentile 수치 (시세·전입인구·통신개통·카드소비 4신호)
  - `[수요강도]`: 콜센터 연결률·전환율 (CALL_CENTER_CONTEXT)
  - `[인기상품]`: 최근 3개월 TOP 3 렌탈 상품 (RENTAL_TREND_CONTEXT)
  - `[교통상황]`: TMAP 평균 속도 기반 혼잡도 (TMAP_SNAPSHOT)
- **출력 파싱**: `result['choices'][0]['messages']::VARCHAR`

---

## 실행 순서

> Snowflake Worksheet에서 순서대로 실행.  
> STEP 3 (모델 학습)은 수 분 소요.

```
STEP 0   : 컬럼 확인 + TMAP 커버리지 진단
STEP 1   : 전처리 테이블 8개 생성
STEP 2   : 고객 프로파일 테이블 생성
STEP 3   : Cortex ML 모델 학습 (4개)
STEP 4   : 이상 탐지 실행 (4개)
STEP 5   : REGION_ALERTS 통합
STEP 6a  : RENTAL_TREND_CONTEXT 생성
STEP 6b  : CALL_CENTER_CONTEXT 생성
STEP 6c  : MARKETING_ALERTS + LLM 문구 생성
STEP 7   : 대시보드 지원 테이블 (SIGNAL_TIMESERIES / SGG_SUMMARY / TELECOM_REGIONAL_TREND)
```

---

## Streamlit 대시보드

`streamlit_app.py` — Snowflake Native App으로 실행.

| 탭 | 내용 |
|----|------|
| 경보 지도 | 서울 동 단위 경보 강도 지도 (DISTRICT_GEOM 폴리곤) |
| 신호 트렌드 | 선택 지역의 4신호 시계열 + 이상 포인트 하이라이트 |
| 마케팅 문구 | 경보 유형별 LLM 생성 광고 카피 + 고객 프로파일 |
| 렌탈/통신 | 서울 구별 통신 개통 트렌드 + 인기 렌탈 상품 |

---

## 심사 배점 대응

| 항목 | 배점 | 대응 |
|------|------|------|
| 창의성 | 25% | 4중 신호 선제 탐지 — 관심 신호 아닌 구조적 시장 신호 |
| Snowflake 전문성 | 25% | Cortex ML × 4 + COMPLETE + Marketplace 4개 + Native App |
| AI (Cortex) | 25% | Anomaly Detection 4모델 체인 → COMPLETE 경보 유형별 맞춤 |
| 현실성 | 15% | 실제 통신사 마케팅 워크플로에 즉시 연결 가능한 구조 |
| 발표 | 10% | 지도 클릭 → 문구 실시간 데모 |
