# Design: MoveRadar — 이사 시즌 선제 마케팅 인텔리전스

Status: v3 (진행 중)
Mode: Builder — Snowflake KR Hackathon 2026

---

## Problem Statement

이사 수요는 아파트 거래가 활성화되는 시점보다 **1~2개월 먼저** 발생한다.
시세 + 전입인구 + 통신 개통 데이터를 결합하면 "지금 집중해야 할 지역"과 "보내야 할 메시지"를
마케팅 담당자보다 빠르게 자동으로 파악할 수 있다.

기존 마케팅 자동화는 CRM 참여 신호 기반 — 이미 관심을 표현한 사람에게만 반응.
MoveRadar는 아직 이사를 인지하지 못한 사람을 **1~2개월 전에 구조적 시장 신호로 탐지**한다.

---

## 심사 배점 역산

| 항목 | 배점 | 우리 전략 |
|------|------|-----------|
| 창의성 | 25% | 4개 Marketplace 데이터셋 교차 — 관심 신호 아닌 구조적 시장 신호 |
| Snowflake 전문성 | 25% | Cortex ML × 3 + COMPLETE + Marketplace 6개 + Native App |
| AI (Cortex) | 25% | Anomaly Detection 3모델 체인 → COMPLETE 경보 유형별 맞춤 문구 |
| 현실성 | 15% | 실제 통신사 마케팅 워크플로에 즉시 연결 가능한 구조 |
| 발표 | 10% | 지도 클릭 → 문구 실시간 데모 |

---

## Constraints

- 마감: 2026-04-12
- 커버리지: 서울 전체 (25개 구 / 400+개 동)
- Snowflake KR Hackathon — 테크 트랙
- 제출물: Forms + 소스 zip + 데모 영상 (10분)

---

## 데이터베이스 전체 현황

### 사용 중인 DB (6개)

---

#### 1. KOREA_REAL_ESTATE_APARTMENT_MARKET_INTELLIGENCE
**스키마**: HACKATHON_2026  
**제공사**: Dataknows

| 테이블 | 행수 규모 | 용도 | 사용 여부 |
|--------|-----------|------|-----------|
| REGION_APT_RICHGO_MARKET_PRICE_M_H | 대용량 | 시세 이상 탐지 시계열 | **사용** |
| REGION_POPULATION_MOVEMENT | 대용량 | 전입인구 이상 탐지 시계열 | **사용** |
| REGION_MOIS_POPULATION_GENDER_AGE_M_H | - | 연령대 프로파일 | 미사용 (ASSET_INCOME_INFO로 대체) |

**왜 사용했나**: 아파트 시세(MEME_PRICE_PER_SUPPLY_PYEONG)와 전입인구(MOVEMENT_TYPE='전입')는 이사 수요를 가장 직접적으로 반영하는 1·2번 신호. 서울 전역 EMD/SGG 단위 월별 시계열 — Cortex Anomaly Detection 입력으로 최적.

**데이터 가용 범위**:
- 훈련: 2021~2023 (서울 25개 구 전체 커버)
- 탐지: 2023~ (22개 구 2023, 3개 구 2023+2024)

---

#### 2. SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS
**스키마**: GRANDATA  
**제공사**: SPH

| 테이블 | 행수 | 용도 | 사용 여부 |
|--------|------|------|-----------|
| M_SCCO_MST | 467 | 행정구역 마스터 + DISTRICT_GEOM 폴리곤 | **사용** |
| ASSET_INCOME_INFO | 269,159 | 소득 구간 프로파일 (KCB 고객군) | **사용** |
| FLOATING_POPULATION_INFO | 2,577,120 | 월별 유동인구 (방문·거주·직장인구) | **미사용 → 4번째 신호 검토 대상** |
| CARD_SALES_INFO | 6,208,957 | 월별 카드 소비 | **미사용 → 5번째 신호 검토 대상** |
| CODE_MASTER | 37 | 코드 마스터 | 불필요 |

**왜 사용했나**:
- `M_SCCO_MST`: 지도 폴리곤(DISTRICT_GEOM)과 동명↔코드 매핑이 필요. Streamlit 지도 렌더링의 기반.
- `ASSET_INCOME_INFO`: 동별 소득 구간 프로파일로 LLM 프롬프트 고객 맥락 생성. RATE_INCOME_OVER_70M으로 고소득/중소득 분류.

**왜 FLOATING_POPULATION_INFO를 안 썼나**:
초기 설계 당시 전입인구(REGION_POPULATION_MOVEMENT)로 이사 수요를 커버할 수 있다고 판단. 그러나 FLOATING_POPULATION_INFO는 **이사 결정 전 임장 활동(방문인구 급증)**을 탐지할 수 있어 전입인구보다 1~2개월 더 선행하는 신호. 컬럼 구조 확인 후 4번째 신호로 추가 예정.

**왜 CARD_SALES_INFO를 안 썼나**:
620만 행 대용량이고 카테고리 코드 체계 파악이 필요. 이사 관련 소비(가구·가전·이사업체) 카테고리 필터링 로직 설계 후 추가 검토.

---

#### 3. SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION
**스키마**: TELECOM_INSIGHTS  
**제공사**: 아정네트웍스

| 테이블 | 행수 규모 | 용도 | 사용 여부 |
|--------|-----------|------|-----------|
| V01_MONTHLY_REGIONAL_CONTRACT_STATS | 대용량 | 통신 개통 이상 탐지 시계열 | **사용** |
| V06_RENTAL_CATEGORY_TRENDS | - | 렌탈 트렌드 (LLM 컨텍스트) | **사용** |
| V09_MONTHLY_CALL_STATS | - | 콜센터 수요 강도 (LLM 컨텍스트) | **사용** |
| V11_CALL_TO_CONTRACT_CONVERSION | - | 콜→계약 전환율 (LLM 컨텍스트) | **사용** |
| V04 외 기타 뷰 | - | 채널 최적화, 해지 분석 등 | 미사용 |

**왜 사용했나**:
- `V01`: 통신 개통(OPEN_COUNT) = 인터넷 신규 개통 = 실제 입주 완료의 직접 증거. 시세·전입인구보다 후행하지만 확정적 신호. 3중 경보 완성의 핵심.
- `V06`: 최근 3개월 렌탈 인기 상품 TOP3를 LLM 프롬프트에 주입 → "정수기·비데·공기청정기" 같은 구체적 상품 언급으로 광고 품질 향상.
- `V09/V11`: 콜센터 연결률·전환율을 수요 강도 지표로 LLM에 주입 → 실제 시장 온도를 광고 문구에 반영.

**왜 V04를 안 썼나**: 채널별 마케팅 최적화는 파이프라인 범위 초과. 경보 탐지 → 문구 생성에 집중.

---

#### 4. KOREA_TRAFFIC_SPEED__VOLUME_DATA__TMAP_NATIONWIDE_COVERAGE
**스키마**: TRAFFIC  
**제공사**: TMAP Mobility

| 테이블 | 행수 | 용도 | 사용 여부 |
|--------|------|------|-----------|
| TMAP_TRAFFIC_VOLUME | 1,244,910 | 도로별 교통량·속도 | **제한적 사용** |

**왜 사용했나/못했나**:
TMAP 데이터가 **2025년 5월 1개월치만 존재** (MONTHS_AVAILABLE=0). Cortex Anomaly Detection 학습을 위한 최소 18개월 데이터 미충족으로 이상 탐지 신호로는 사용 불가.

대신 서울 구별 평균 속도(AVG_SPEED_KMPH)를 TMAP_SNAPSHOT 테이블로 집계하여 LLM 프롬프트에 **교통 혼잡도(혼잡/보통/원활)** 컨텍스트로 주입. 이사 당일 교통 상황 반영한 광고 문구 생성에 활용.

---

### 미사용 DB (2개)

---

#### 5. POPULATION_MOBILITY_CARD_TRANSACTIONS_ASSETS_AND_INCOME_STATISTICS_SOUTH_KOREA_ADMINISTRATIVE_BOUNDARIES
**스키마**: MRKT_SAMPLE

| 테이블 | 행수 | 내용 |
|--------|------|------|
| M_SCCO_FLTNG_PPLTN | 1,568 | 유동인구 |
| M_SCCO_CARD_CNSM | 5,713 | 카드소비 |
| M_SCCO_ASET_INCM | 8 | 자산소득 |
| M_SCCO_MST | 14 | 행정구역 |

**왜 안 쓰나**: 스키마 코멘트 "2023.12 서울 강남구 MZ세대" — **샘플 데이터**. 강남구 1개월치만 존재. 서울 전역 파이프라인에 사용 불가. 동일한 테이블의 서울 전역 버전은 `SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS`에 있음.

---

#### 6. KOREAN_POPULATION__APARTMENT_MARKET_PRICE_DATA
**스키마**: 미확인

**왜 안 쓰나**: `KOREA_REAL_ESTATE_APARTMENT_MARKET_INTELLIGENCE`가 동일 계열의 더 상세한 해커톤 전용 데이터셋으로 제공됨. 중복 소스 사용 불필요. 향후 커버리지 보완이 필요할 경우 검토.

---

## 신호 설계 (현재 v3)

| 순서 | 신호 | 테이블 | 의미 | 가중치 | 선행성 |
|------|------|--------|------|--------|--------|
| 1 | **시세** | REGION_APT_RICHGO_MARKET_PRICE_M_H | 아파트 거래가 이상 = 수요 급변 | 0.50 | 선행 1~2개월 |
| 2 | **전입인구** | REGION_POPULATION_MOVEMENT | 실제 이사 인구 급증 | 0.30 | 동시 |
| 3 | **통신 개통** | V01_MONTHLY_REGIONAL_CONTRACT_STATS | 인터넷 신규 개통 = 입주 완료 증거 | 0.20 | 후행 1개월 |

**COMBINED_SCORE** = |price_pct−0.5|×2×0.50 + |pop_pct−0.5|×2×0.30 + |tc_pct−0.5|×2×0.20  
**경보 임계값**: PERCENTILE > 0.75 또는 < 0.25  
**경보 유형**: 7가지 (3중 동시 / 2중 조합 3가지 / 단일 3가지)

---

## 다음 추가 검토 신호

| 신호 | 테이블 | 예상 선행성 | 추가 이유 |
|------|--------|------------|-----------|
| **유동인구** | FLOATING_POPULATION_INFO | 선행 2~3개월 | 이사 전 임장(방문) 활동 탐지 — 전입인구보다 더 앞선 신호 |
| **카드소비** | CARD_SALES_INFO | 선행 1~2개월 | 이사 관련 소비(가구·가전·이사업체) 급증 탐지 |

컬럼 구조 확인 후 pipeline.sql에 추가 예정:
```sql
SELECT * FROM SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.FLOATING_POPULATION_INFO LIMIT 3;
SELECT * FROM SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS.GRANDATA.CARD_SALES_INFO LIMIT 3;
```

---

## LLM 컨텍스트 설계

| 컨텍스트 | 소스 | 역할 |
|----------|------|------|
| [시장신호] | REGION_ALERTS (percentile 수치) | 경보 강도를 수치로 표현 |
| [고객] | CUSTOMER_PROFILE_VIEW | 연령대·소득 맞춤 톤 설정 |
| [수요강도] | CALL_CENTER_CONTEXT (V09+V11) | 실제 문의 증가율로 시장 온도 반영 |
| [인기상품] | RENTAL_TREND_CONTEXT (V06) | 최근 3개월 TOP3 렌탈 상품 구체 언급 |
| [교통상황] | TMAP_SNAPSHOT | 교통 혼잡도로 이사 당일 현장감 반영 |

---

## 아키텍처 (v3 현재)

```
[Snowflake Marketplace — 4개 DB 실제 사용]

Dataknows  ──────────────────────────────────────────────────────────┐
  REGION_APT_RICHGO_MARKET_PRICE_M_H  →  시세 시계열                │
  REGION_POPULATION_MOVEMENT          →  전입인구 시계열              │
                                                                      │
SPH (GRANDATA)  ─────────────────────────────────────────────────────┤
  M_SCCO_MST          →  폴리곤 + 지역명 매핑                        │
  ASSET_INCOME_INFO   →  소득 구간 프로파일                          │
  FLOATING_POPULATION_INFO  →  [검토 중] 유동인구 선행 신호           │
  CARD_SALES_INFO           →  [검토 중] 카드소비 선행 신호           │
                                                                      │
아정네트웍스  ────────────────────────────────────────────────────────┤
  V01  →  통신 개통 시계열                                           │
  V06  →  렌탈 트렌드 (LLM 컨텍스트)                                │
  V09/V11  →  콜센터 수요강도 (LLM 컨텍스트)                        │
                                                                      │
TMAP  ────────────────────────────────────────────────────────────────┤
  TMAP_TRAFFIC_VOLUME  →  교통 혼잡도 스냅샷 (LLM 컨텍스트)          │
  (이상 탐지 불가 — 데이터 1개월)                                    │
                                                                      ↓
[STEP 1] 전처리 (훈련/탐지 분리)
  price_timeseries_train/detect    ←  훈련 2021~2023 / 탐지 2023~
  pop_timeseries_train/detect      ←  훈련 2021~2023 / 탐지 2023~
  telecom_timeseries_train/detect  ←  훈련 ~2023 / 탐지 2023~
  TMAP_SNAPSHOT                    ←  현재 월 구별 평균속도 스냅샷
                  ↓
[STEP 2] 고객 프로파일
  customer_profile_view  ←  M_SCCO_MST × ASSET_INCOME_INFO
                  ↓
[STEP 3] Cortex Anomaly Detection 학습 (3개 모델)
[STEP 4] 이상 탐지 실행 → PRICE / POP / TELECOM_ANOMALY_RESULTS
                  ↓
[STEP 5] REGION_ALERTS
  COMBINED_SCORE = price×0.50 + pop×0.30 + telecom×0.20
  경보 유형 7가지 (3중 ~ 단일)
                  ↓
[STEP 6] Cortex COMPLETE → MARKETING_ALERTS
  컨텍스트: 렌탈 트렌드 + 콜센터 수요강도 + TMAP 교통상황 + percentile 수치
                  ↓
[STEP 7] 대시보드 지원 테이블
  SIGNAL_TIMESERIES / SGG_SUMMARY / TELECOM_REGIONAL_TREND
                  ↓
[Streamlit Native App]
  탭1: 경보 지도  탭2: 신호 트렌드  탭3: 마케팅 문구  탭4: 렌탈/통신
```
