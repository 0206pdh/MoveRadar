# Design: MoveRadar — 이사 시즌 선제 마케팅 인텔리전스

Status: v4 (카드소비 4신호 통합 완료)
Mode: Builder — Snowflake KR Hackathon 2026

---

## Problem Statement

이사 수요는 아파트 거래가 활성화되는 시점보다 **1~2개월 먼저** 발생한다.
기존 마케팅 자동화는 CRM 참여 신호 기반 — 이미 관심을 표현한 사람에게만 반응.
MoveRadar는 아직 이사를 인지하지 못한 사람을 **1~2개월 전에 구조적 시장 신호로 탐지**한다.

---

## 심사 배점 역산

| 항목 | 배점 | 우리 전략 |
|------|------|-----------|
| 창의성 | 25% | 4신호(시세·전입인구·통신·카드소비) 교차 — 관심 신호 아닌 구조적 시장 신호 |
| Snowflake 전문성 | 25% | Cortex ML × 5모델 + COMPLETE + Marketplace 5개 DB + Native App |
| AI (Cortex) | 25% | Anomaly Detection 4+1모델 체인 → COMPLETE 15가지 경보 유형별 맞춤 문구 |
| 현실성 | 15% | 실제 통신사 마케팅 워크플로에 즉시 연결 가능한 구조 |
| 발표 | 10% | 구 선택 → 4신호 트렌드 + LLM 문구 실시간 데모 |

---

## Constraints

- 마감: 2026-04-12
- 커버리지: 서울 전체 (25개 구)
- Snowflake KR Hackathon — 테크 트랙
- 제출물: Forms + 소스 zip + 데모 영상 (10분)

---

## 데이터베이스 전체 현황

### 사용 중인 DB (5개)

---

#### 1. KOREA_REAL_ESTATE_APARTMENT_MARKET_INTELLIGENCE
**스키마**: HACKATHON_2026 | **제공사**: Dataknows

| 테이블 | 용도 | 사용 여부 |
|--------|------|-----------|
| REGION_APT_RICHGO_MARKET_PRICE_M_H | 시세 이상 탐지 (신호 1) | **사용** |
| REGION_POPULATION_MOVEMENT | 전입인구 이상 탐지 (신호 2) | **사용** |
| REGION_MOIS_POPULATION_GENDER_AGE_M_H | 연령대 프로파일 | 미사용 (ASSET_INCOME_INFO로 대체) |

**왜 사용했나**:
- `REGION_APT_RICHGO_MARKET_PRICE_M_H`: 아파트 실거래 시세(MEME_PRICE_PER_SUPPLY_PYEONG)는 이사 수요의 가장 강한 선행 신호. 거래 급등 = 단기 내 입주 수요 폭발. 전체 서울 EMD 단위 월별 시계열로 Cortex Anomaly Detection 입력으로 최적.
- `REGION_POPULATION_MOVEMENT`: 전입인구(MOVEMENT_TYPE='전입')는 이사의 직접 증거. 시세가 1~2개월 선행, 전입인구는 실제 이사 발생 시 급등.

**데이터 가용 범위 및 훈련/탐지 분리 전략**:
- 22개 구: 마켓플레이스에서 2023년까지만 데이터 제공
  - 시세 훈련: 2021~2022 (`price_train_22sgg`, `< 2023-01-01`)
  - 시세 탐지: 2023~ (`price_detect_22sgg`, `>= 2023-01-01`)
- 영등포구·서초구·중구 (3개 구): 2024년 이후 데이터 추가 제공
  - 시세 훈련: 2021~2023 (`price_train_3sgg`, `< 2024-01-01`)
  - 시세 탐지: 2024~ (`price_detect_3sgg`, `>= 2024-01-01`)
- 전입인구 훈련: 2021~2022 (`< 2023-01-01`), 탐지: 2023~ (`>= 2023-01-01`)
- **이유**: Cortex Anomaly Detection의 "All evaluation timestamps must be after last training timestamp" 제약 때문에 SGG별 데이터 가용 기간에 맞게 커트오프 분리 필수

---

#### 2. SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS
**스키마**: GRANDATA | **제공사**: SPH

| 테이블 | 행수 | 용도 | 사용 여부 |
|--------|------|------|-----------|
| M_SCCO_MST | 467 | 행정구역 마스터 + DISTRICT_GEOM 폴리곤 | **사용** |
| ASSET_INCOME_INFO | 269,159 | 소득 구간 프로파일 | **사용** |
| CARD_SALES_INFO | 6,208,957 | 카드소비 이사지표 (신호 4) | **사용** |
| FLOATING_POPULATION_INFO | 2,577,120 | 월별 유동인구 | 미사용 (신호 5 추가 여건 시) |
| CODE_MASTER | 37 | 코드 마스터 | 불필요 |

**왜 사용했나**:
- `M_SCCO_MST`: 지도 폴리곤(DISTRICT_GEOM)과 동명↔코드 매핑의 기반. CITY_KOR_NAME(구명)과 DISTRICT_KOR_NAME(동명)으로 SGG_EMD 키 생성.
- `ASSET_INCOME_INFO`: 동별 소득 구간 프로파일(RATE_INCOME_OVER_70M)로 고소득/중소득 분류 → LLM 프롬프트에 "고소득 지역" 같은 고객 맥락 주입.
- `CARD_SALES_INFO`: ELECTRONICS_FURNITURE_SALES(가전·가구) + HOME_LIFE_SERVICE_SALES(생활서비스) + LARGE_DISCOUNT_STORE_SALES(대형마트) 합산 = 이사 준비 소비 지표. 실제 이사 1~2개월 전 이 카테고리 카드 소비가 급증하는 패턴을 이상 탐지로 포착.

**왜 FLOATING_POPULATION_INFO를 안 썼나**: 전입인구(REGION_POPULATION_MOVEMENT)로 이사 수요를 커버하므로 우선순위 후순위. 유동인구는 임장 활동(이사 전 지역 답사)을 탐지할 수 있어 전입인구보다 더 선행하는 신호지만, 시간 제약으로 4신호 완성 후 검토.

---

#### 3. SOUTH_KOREA_TELECOM_SUBSCRIPTION_ANALYTICS__CONTRACTS_MARKETING_AND_CALL_CENTER_INSIGHTS_BY_REGION
**스키마**: TELECOM_INSIGHTS | **제공사**: 아정네트웍스

| 테이블 | 용도 | 사용 여부 |
|--------|------|-----------|
| V01_MONTHLY_REGIONAL_CONTRACT_STATS | 통신 개통 이상 탐지 (신호 3) | **사용** |
| V06_RENTAL_CATEGORY_TRENDS | 렌탈 트렌드 (LLM 컨텍스트) | **사용** |
| V09_MONTHLY_CALL_STATS | 콜센터 수요 강도 (LLM 컨텍스트) | **사용** |
| V11_CALL_TO_CONTRACT_CONVERSION | 콜→계약 전환율 (LLM 컨텍스트) | **사용** |
| V04 외 기타 뷰 | 채널 최적화, 해지 분석 등 | 미사용 |

**왜 사용했나**:
- `V01` (OPEN_COUNT): 인터넷 신규 개통 = 실제 입주 완료의 직접 증거. 시세·전입인구는 이사 예측 신호지만 통신 개통은 입주 확정 신호. 3가지 시점(예측→진행→완료)을 모두 포착하는 체인의 핵심.
  - 훈련: 2023년 데이터 (`telecom_timeseries_train`, `< 2024-01-01`)
  - 탐지: 2024~  (`telecom_timeseries_detect`, `>= 2024-01-01`)
  - 이유: V01 데이터 자체가 2023년부터 시작 → 2023을 훈련, 2024+를 탐지로 분리
- `V06`: TOP 렌탈 상품을 LLM에 주입해 "정수기·비데·공기청정기" 같은 구체적 상품 언급으로 광고 품질 향상.
- `V09/V11`: 콜센터 연결률·전환율을 수요강도 지표로 LLM에 주입 → 실제 시장 온도를 광고 문구에 반영.

**왜 V04를 안 썼나**: 채널별 마케팅 최적화는 파이프라인 범위 초과. 경보 탐지 → 문구 생성에 집중.

---

#### 4. KOREA_TRAFFIC_SPEED__VOLUME_DATA__TMAP_NATIONWIDE_COVERAGE
**스키마**: TRAFFIC | **제공사**: TMAP Mobility

| 테이블 | 행수 | 용도 | 사용 여부 |
|--------|------|------|-----------|
| TMAP_TRAFFIC_VOLUME | 1,244,910 | 교통 혼잡도 컨텍스트 | **제한적 사용** |

**왜 이상 탐지 신호로 쓰지 못했나**: 마켓플레이스에서 **2025년 5월 단 1개월치만 제공** (MONTHS_AVAILABLE=0). Cortex Anomaly Detection은 최소 18개월 이상 시계열 필요 → 이상 탐지 불가.

**어떻게 활용하나**: 서울 구별 현재 평균 속도(AVG_SPEED_KMPH)를 `TMAP_SNAPSHOT`으로 집계. LLM 프롬프트에 `[교통상황] 교통 혼잡/보통/원활` 컨텍스트 주입 → 이사 당일 교통 상황 반영한 현장감 있는 광고 문구 생성.

---

### 미사용 DB (2개)

---

#### 5. POPULATION_MOBILITY_CARD_TRANSACTIONS_ASSETS_AND_INCOME_STATISTICS_SOUTH_KOREA_ADMINISTRATIVE_BOUNDARIES
**스키마**: MRKT_SAMPLE

**왜 안 쓰나**: 스키마 코멘트 "2023.12 서울 강남구 MZ세대" — **샘플 데이터**. 강남구 1개월치(1,568행)만 존재. 서울 전역 파이프라인에 사용 불가. 동일 테이블의 전체 버전은 `SEOUL_DISTRICTLEVEL_DATA_FLOATING_POPULATION_CONSUMPTION_AND_ASSETS`에 있음.

---

#### 6. KOREAN_POPULATION__APARTMENT_MARKET_PRICE_DATA

**왜 안 쓰나**: `KOREA_REAL_ESTATE_APARTMENT_MARKET_INTELLIGENCE`가 동일 계열의 더 상세한 해커톤 전용 데이터셋으로 제공됨. 중복 소스 불필요. 향후 커버리지 보완이 필요할 경우 검토.

---

## 신호 설계 (v4 — 4신호 통합)

| 순서 | 신호 | 테이블 | 의미 | 가중치 | 선행성 |
|------|------|--------|------|--------|--------|
| 1 | **시세** | REGION_APT_RICHGO_MARKET_PRICE_M_H | 아파트 거래가 이상 급등/급락 = 이사 수요 급변 | **0.40** | 선행 1~2개월 |
| 2 | **전입인구** | REGION_POPULATION_MOVEMENT | 실제 이사 인구 급증 = 이사 발생 직접 증거 | **0.25** | 동시~후행 1개월 |
| 3 | **통신 개통** | V01_MONTHLY_REGIONAL_CONTRACT_STATS | 인터넷 신규 개통 = 입주 완료 확정 증거 | **0.20** | 후행 1개월 |
| 4 | **카드소비** | CARD_SALES_INFO (가전+생활서비스+대형마트) | 이사 준비 구매 급증 = 이사 결정 직후 소비 선행 | **0.15** | 선행 1~2개월 |

---

## 가중치 설정 이유

**0.40 (시세)**: 아파트 시세 급등은 이사 수요의 가장 강력하고 명확한 선행 지표. 투자 목적 매입→전세/월세 물량 증가→이사 수요 폭발 경로가 분명. 데이터 품질도 EMD 단위 월별로 가장 세밀.

**0.25 (전입인구)**: 실제 이사 인구는 수요 확정 증거지만, 이미 이사가 발생한 후의 신호라 선행성이 낮음. 그러나 "지금 이 지역이 이사 목적지인가"를 가장 직접적으로 보여주는 지표라 2순위 가중치 부여.

**0.20 (통신 개통)**: 입주 직후 인터넷 개통은 이사 완료의 확정 신호. 후행 신호지만 통신사 마케팅의 핵심 접점(개통 타이밍)이라 높은 가중치. 이 신호가 발동하면 마케팅 타이밍이 임박했다는 의미.

**0.15 (카드소비)**: 가전·가구·대형마트 소비 급증은 이사 결정 직후 발생하는 준비 소비. 시세와 함께 1~2개월 선행하지만, 이사와 무관한 소비도 포함될 수 있어 노이즈 감안해 최저 가중치.

---

## 경보 임계값 설정 이유

**PERCENTILE > 0.75 (급등) 또는 < 0.25 (급락)**

Cortex Anomaly Detection이 출력하는 PERCENTILE은 0~1 사이의 상대적 위치. 75%ile 초과 = 상위 25% 수준의 이상 급등, 25%ile 미만 = 하위 25% 수준의 이상 급락.

- **왜 0.75/0.25인가**: 표준 IQR 방식(25~75%)의 경계값 채택. 단순 이상치(outlier) 기준인 1.5×IQR보다 엄격하지 않아 중간 강도 이상도 포착.
- **왜 급락도 탐지하나**: 시세 급락 = 매도 폭발(이사 나감), 전입인구 급락 = 이탈 급증. 이사 관련 수요는 급등뿐 아니라 급락도 마케팅 기회(전출 지역의 신규 입주민 유치).
- **COMBINED_SCORE > 0.25 기준 (MARKETING_ALERTS 필터)**: 단일 신호 최대 기여값이 0.5×2=1.0×가중치이므로, 0.25는 "1개 신호가 최대 강도의 50% 이상" 또는 "복수 신호가 약한 이상"에 해당하는 최소 의미 있는 경보 수준.

---

## COMBINED_SCORE 수식

```
COMBINED_SCORE = ABS(price_pct  - 0.5) × 2 × 0.40
               + ABS(pop_pct    - 0.5) × 2 × 0.25
               + ABS(telecom_pct- 0.5) × 2 × 0.20
               + ABS(card_pct   - 0.5) × 2 × 0.15
```

- `ABS(x - 0.5) × 2`: 0.5(중립)에서의 이탈 거리를 0~1로 정규화. PERCENTILE=1.0이면 기여값=1.0, PERCENTILE=0.0이면 기여값=1.0, PERCENTILE=0.5이면 기여값=0.
- 데이터 없는 신호는 COALESCE(..., 0.5) → 중립값으로 처리해 점수에 기여 없음.
- 최대값: 1.0 (4개 신호 모두 최대 강도)

---

## 훈련/탐지 분리 전략 (train/detect split)

Cortex Anomaly Detection은 **탐지 데이터의 모든 타임스탬프가 훈련 데이터의 마지막 타임스탬프 이후**여야 함. 신호별로 데이터 가용 기간이 다르기 때문에 각각 다른 커트오프 적용.

| 신호 | 훈련 기간 | 탐지 기간 | 비고 |
|------|-----------|-----------|------|
| 시세 (22개 구) | ~2022-12 | 2023-01~ | 22개 구 마켓플레이스 데이터 2023까지만 제공 |
| 시세 (3개 구) | ~2023-12 | 2024-01~ | 영등포·서초·중구 2024 데이터 추가 제공 |
| 전입인구 | ~2022-12 | 2023-01~ | 시세와 동일 소스, 동일 커트오프 |
| 통신 개통 | ~2023-12 | 2024-01~ | V01 데이터 2023년부터 시작 → 2023 훈련, 2024+ 탐지 |
| 카드소비 | ~2022-12 | 2023-01~ | CARD_SALES_INFO 2024 데이터 미제공 |

**서울 25개 구 전역 커버 방식**: REGION_ALERTS의 드라이버 테이블을 POP_ANOMALY_RESULTS(25개 구 × 2023~ 전체 커버)로 설정. 시세는 SGG 집계 후 LEFT JOIN. 시세 데이터가 없는 구는 price_pct=0.5(중립)로 처리.

---

## LLM 프롬프트 설계

**모델**: `mistral-large2` (Snowflake Cortex COMPLETE)  
**포맷**: messages array (system + user role 분리)  
**Temperature**: 0.9 (광고 문구 다양성 확보)

| 컨텍스트 변수 | 소스 | 역할 |
|--------------|------|------|
| `[지역]` | REGION_ALERTS.SGG + ALERT_DATE | 어느 지역, 어느 시점 |
| `[고객]` | customer_profile_view | 연령대·소득 맞춤 톤 설정 |
| `[경보강도]` | COMBINED_SCORE + 각 신호 percentile 수치 | 신호 강도를 구체적 수치로 |
| `[수요강도]` | CALL_CENTER_CONTEXT (V09+V11) | 실제 콜센터 연결률·전환율 |
| `[인기상품]` | RENTAL_TREND_CONTEXT (V06) | TOP3 렌탈 상품 구체 언급 |
| `[교통상황]` | TMAP_SNAPSHOT | 이사 당일 교통 혼잡도 |

**15가지 경보 유형별 맞춤 프롬프트**:

| 경보 유형 | 강조 포인트 |
|-----------|------------|
| 4중 동시 경보 | 4신호 모두 수치 포함, 전체 혜택 패키지 |
| 3중 동시 경보 | 시세·전입인구·통신 3개 수치 + 교통상황 |
| 시세+전입인구+카드 경보 | 이사 준비 소비 언급 + 사전 예약 강조 |
| 시세+통신+카드 경보 | 프리미엄·빠른 설치 강조 |
| 전입인구+통신+카드 경보 | 입주 완료 직후 타이밍 강조 |
| 시세+전입인구 경보 | 이사 당일 렌탈 설치 + 기가 인터넷 결합 |
| 시세+통신 경보 | 인터넷 당일 개통 필수 언급 |
| 시세+카드 경보 | 이사 준비 시점·사전 계약 강조 |
| 전입인구+통신 경보 | 인터넷+렌탈 결합 할인 |
| 전입인구+카드 경보 | 새 생활 시작 감성 |
| 통신+카드 경보 | 입주 완료 직후 렌탈 즉시 설치 |
| 시세 경보 | 프리미엄·속도 강조 |
| 전입인구 경보 | 새집 시작 감성 |
| 통신 경보 | 개통 속도·첫 달 혜택 |
| 카드소비 경보 | 이사 준비 단계·사전 계약 혜택 |

---

## 아키텍처 (v4 현재)

```
[Snowflake Marketplace — 5개 DB 활용]

Dataknows  ──────────────────────────────────────────────────────┐
  REGION_APT_RICHGO_MARKET_PRICE_M_H  →  시세 이상 탐지 (신호1) │
  REGION_POPULATION_MOVEMENT          →  전입인구 탐지 (신호2)   │
                                                                  │
SPH (GRANDATA)  ─────────────────────────────────────────────────┤
  M_SCCO_MST          →  폴리곤 + 지역명 매핑                    │
  ASSET_INCOME_INFO   →  소득 구간 프로파일                      │
  CARD_SALES_INFO     →  카드소비 이사지표 탐지 (신호4)           │
                                                                  │
아정네트웍스  ────────────────────────────────────────────────────┤
  V01  →  통신 개통 이상 탐지 (신호3)                            │
  V06  →  렌탈 트렌드 (LLM 컨텍스트)                            │
  V09/V11  →  콜센터 수요강도 (LLM 컨텍스트)                    │
                                                                  │
TMAP  ───────────────────────────────────────────────────────────┤
  TMAP_TRAFFIC_VOLUME  →  교통 혼잡도 스냅샷 (LLM 컨텍스트)      │
  ※ 데이터 1개월치 → 이상 탐지 불가, 컨텍스트 전용              │
                                                                  ↓
[STEP 1] 전처리 (훈련/탐지 분리 — 신호별 커트오프 상이)
  price_train_22sgg / price_detect_22sgg  ←  22개 구 훈련~2022 / 탐지2023~
  price_train_3sgg  / price_detect_3sgg   ←  3개 구  훈련~2023 / 탐지2024~
  pop_timeseries_train/detect             ←  훈련~2022 / 탐지2023~
  telecom_timeseries_train/detect         ←  훈련~2023 / 탐지2024~
  card_timeseries_train/detect            ←  훈련~2022 / 탐지2023~
  TMAP_SNAPSHOT                           ←  현재 월 구별 평균속도
                  ↓
[STEP 2] 고객 프로파일
  customer_profile_view  ←  M_SCCO_MST × ASSET_INCOME_INFO
                  ↓
[STEP 3] Cortex Anomaly Detection 학습 (5개 모델)
  price_anomaly_model_22  (22개 구)
  price_anomaly_model_3   (영등포·서초·중구)
  pop_anomaly_model
  telecom_anomaly_model
  card_anomaly_model
                  ↓
[STEP 4] 이상 탐지 실행
  PRICE_ANOMALY_RESULTS   = model_22 UNION ALL model_3 → 25개 구 전역
  POP_ANOMALY_RESULTS     (25개 구)
  TELECOM_ANOMALY_RESULTS (25개 구)
  CARD_ANOMALY_RESULTS    (25개 구)
                  ↓
[STEP 5] REGION_ALERTS (POP 드라이버 → 25개 구 보장)
  COMBINED_SCORE = price×0.40 + pop×0.25 + telecom×0.20 + card×0.15
  15가지 ALERT_TYPE (4중 동시 ~ 단일 경보)
                  ↓
[STEP 6] Cortex COMPLETE → MARKETING_ALERTS
  15가지 경보 유형별 맞춤 LLM 프롬프트
  컨텍스트: 4신호 percentile + 렌탈 트렌드 + 콜센터 수요강도 + 교통상황
                  ↓
[STEP 7] 대시보드 지원 테이블
  SIGNAL_TIMESERIES (4신호 시계열 통합)
  SGG_SUMMARY (QUAD/TRIPLE/PRICE/POP/TELECOM/CARD 경보 KPI)
  TELECOM_REGIONAL_TREND
                  ↓
[Streamlit Native App — 4탭]
  탭1: 경보 현황 (25개 구 × 15경보유형 + 4신호 점수)
  탭2: 신호 트렌드 (4신호 시계열 + 이상 포인트)
  탭3: 통신 분석 (렌탈 TOP10 + 콜센터 트렌드 + 구별 통신 개통)
  탭4: 마케팅 문구 (경보 유형별 LLM 카피 + 평균 점수 비교)
```
