# MoveRadar 진행 가이드

Snowflake Marketplace에서 데이터셋을 붙이는 단계부터, 실제로 `design.md` 기준 작업을 어디서부터 어떻게 진행해야 하는지 정리한 문서입니다.

---

## 1. 목표 다시 정리

이 프로젝트의 첫 목표는 단순히 데이터를 조회하는 것이 아니라, 아래 흐름을 실제로 Snowflake 안에서 연결하는 것입니다.

1. Marketplace 데이터셋 접근 확보
2. 필요한 테이블 확인
3. EDA 실행
4. 지역 단위 결정(법정동 EMD 유지 vs 시군구 SGG fallback)
5. 조인 키 검증
6. 전처리 뷰 생성
7. Cortex Anomaly Detection 실행
8. 경보 테이블 생성
9. 마케팅 문구 생성
10. Streamlit 대시보드 연결

즉, 지금 단계에서는 **Marketplace에서 데이터셋을 붙인 뒤, design 문서의 The Assignment와 아키텍처를 실제 테이블 기준으로 맞춰가는 작업**이 핵심입니다.

---

## 2. 지금 네 상황 정리

현재까지 확인된 내용은 다음과 같습니다.

- `COMPUTE_WH` warehouse는 이미 있음
- SQL worksheet도 열 수 있음
- 처음엔 `DATAKNOWS.PUBLIC...`, `SPH.PUBLIC...` 경로로 조회하려 했는데 권한/존재 오류가 남
- Marketplace에서 가져온 뒤 예시 worksheet가 열렸고, 여기서 `HACKATHON_2026.<table>` 형태의 테이블 예시가 보임

이 말은 보통 다음 중 하나입니다.

- 해커톤용 데이터셋이 개별 DB(`DATAKNOWS`, `SPH`)로 직접 보이는 게 아니라
- **`HACKATHON_2026` 같은 통합 데이터베이스/공유 데이터베이스 안에 묶여 제공되는 구조**일 수 있음

따라서 이제부터는 문서에 적힌 논리를 유지하되, **실제 Snowflake에 붙은 DB/Schema/Table 이름 기준으로 다시 확인**해야 합니다.

---

## 3. Marketplace에서 해야 하는 일

### 3-1. Marketplace 진입

왼쪽 메뉴에서:

- `Marketplace`

로 들어갑니다.

### 3-2. 필요한 공급자 / 데이터 찾기

검색창에서 아래 키워드를 사용합니다.

- `Dataknows`
- `SPH`
- `AJDNETWORKS`
- 또는 해커톤 페이지에서 제공한 통합 데이터셋 이름

지금 화면상으로는 SPH 쪽 데이터 상품 목록이 보이고 있습니다.

### 3-3. 데이터셋 상세 페이지 확인

각 데이터셋 상세 페이지에서 먼저 볼 것:

- Free / Free to try / By request 여부
- Instantly accessible 여부
- 제공되는 예시 SQL / example worksheet 존재 여부
- 실제 어떤 테이블이 포함되어 있는지

### 3-4. Get / Install / Add 실행

보통 버튼 이름은 아래 중 하나입니다.

- `Get`
- `Get data`
- `Install`
- `Add`
- `View in Snowsight`

이걸 누르면 네 계정에 공유 데이터가 붙습니다.

### 3-5. 붙은 뒤 바로 해야 할 것

Marketplace에서 가져온 뒤 자동으로 예시 worksheet가 열릴 수 있습니다.

이때 해야 할 일은:

1. 예시 worksheet가 정상적으로 열리는지 확인
2. 예시 SQL 안의 DB 이름 확인
3. 그 DB 이름을 기준으로 실제 테이블 목록 조회

중요한 점은, **문서에 적힌 DB 이름이 실제 계정에 붙은 이름과 다를 수 있다**는 것입니다.

---

## 4. Warehouse는 어떻게 쓰면 되나

현재는 `COMPUTE_WH`가 이미 있으므로 새로 만들 필요는 없습니다.

SQL 시작할 때는 보통 아래처럼 먼저 선언해두면 됩니다.

```sql
USE WAREHOUSE COMPUTE_WH;
```

필요하면 현재 상태 확인:

```sql
SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE();
```

---

## 5. Marketplace로 붙은 데이터 구조 확인 순서

Marketplace에서 데이터셋을 가져온 직후 가장 먼저 아래 순서로 확인합니다.

```sql
USE WAREHOUSE COMPUTE_WH;
SHOW DATABASES;
```

그 다음 실제로 예시 SQL에서 보인 DB가 있다면:

```sql
SHOW SCHEMAS IN DATABASE HACKATHON_2026;
SHOW TABLES IN DATABASE HACKATHON_2026;
```

테이블이 많으면 이름 패턴으로 줄여서 찾습니다.

```sql
SHOW TABLES LIKE '%REGION_APT%' IN DATABASE HACKATHON_2026;
SHOW TABLES LIKE '%FLOATING%' IN DATABASE HACKATHON_2026;
SHOW TABLES LIKE '%SCCO%' IN DATABASE HACKATHON_2026;
SHOW TABLES LIKE '%INCOME%' IN DATABASE HACKATHON_2026;
SHOW TABLES LIKE '%V01%' IN DATABASE HACKATHON_2026;
SHOW TABLES LIKE '%POPULATION%' IN DATABASE HACKATHON_2026;
```

이 단계 목적은 간단합니다.

- design 문서에 적은 테이블이 실제로 존재하는지 확인
- 이름이 다르면 실제 이름으로 매핑
- schema가 `PUBLIC`인지 아닌지도 확인

---

## 6. design.md 기준으로 찾아야 하는 핵심 테이블

설계 문서 기준으로 필요한 주요 데이터는 다음입니다.

### 6-1. 시세 데이터

목표:

- 아파트 지역 시세 시계열 확보
- anomaly detection용 입력 생성

문서상 기대 테이블:

- `REGION_APT_RICHGO_MARKET_PRICE_M_H`

### 6-2. 유동인구 데이터

목표:

- 지역 방문 인구 시계열 확보
- anomaly detection용 입력 생성

문서상 기대 테이블:

- `FLOATING_POPULATION_INFO`

### 6-3. 행정구역 마스터

목표:

- 코드 ↔ 지역명 매핑
- 지오메트리(지도 폴리곤) 확보

문서상 기대 테이블:

- `M_SCCO_MST`

### 6-4. 고객 프로파일용 데이터

목표:

- 연령대, 소득 구간 묶기

문서상 기대 테이블:

- `REGION_MOIS_POPULATION_GENDER_AGE_M_H`
- `ASSET_INCOME_INFO`

### 6-5. 과거 계약 패턴 검증용 데이터

목표:

- 실제 이사/설치 계약 패턴과 구조적 신호 연결

문서상 기대 테이블:

- `V01`

---

## 7. 예시 worksheet가 열렸을 때 해석하는 법

지금처럼 예시 worksheet에 아래 같은 SQL이 보인다면:

```sql
FROM HACKATHON_2026.REGION_APT_RICHGO_MARKET_PRICE_M_H
```

이건 보통 다음 의미입니다.

- 해당 테이블이 실제로 사용 가능한 후보임
- 공급자가 추천하는 참조 경로임
- design 문서의 원래 경로 대신 이 경로를 써야 할 가능성이 큼

즉, 이제는 `DATAKNOWS.PUBLIC...`를 고집하지 말고, **실제 worksheet에서 보이는 경로를 우선 소스 오브 트루스로 삼는 게 맞습니다.**

---

## 8. 지금 바로 실행할 1차 SQL 세트

아래는 네가 지금 바로 붙여넣어서 확인할 기본 SQL입니다.

```sql
USE WAREHOUSE COMPUTE_WH;
SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE();
SHOW DATABASES;
SHOW SCHEMAS IN DATABASE HACKATHON_2026;
SHOW TABLES IN DATABASE HACKATHON_2026;
```

그 다음 시세 테이블 바로 확인:

```sql
SELECT COUNT(*), MIN(YYYYMMDD::DATE), MAX(YYYYMMDD::DATE)
FROM HACKATHON_2026.REGION_APT_RICHGO_MARKET_PRICE_M_H
WHERE SGG IN ('서초구', '영등포구', '중구');
```

법정동별 포인트 수 확인:

```sql
SELECT SGG, EMD, COUNT(DISTINCT YYYYMMDD) AS data_points
FROM HACKATHON_2026.REGION_APT_RICHGO_MARKET_PRICE_M_H
WHERE SGG IN ('서초구', '영등포구', '중구')
  AND REGION_LEVEL = 'emd'
GROUP BY 1, 2
ORDER BY 3;
```

이 두 개가 돌아가면 design 문서의 첫 과제 중 절반은 통과한 겁니다.

---

## 9. The Assignment를 실제 DB 기준으로 다시 실행하는 방법

설계 문서의 The Assignment는 원래 아래 목적이 있습니다.

1. 데이터 볼륨 확인
2. 날짜 범위 확인
3. 법정동별 시계열 포인트 확인

그런데 지금 실제 Snowflake에 붙은 경로가 다를 수 있으니, **목표는 동일하게 유지하되 FROM 절만 실제 경로로 치환**해야 합니다.

예를 들어 원래 문서에 이런 식이 있었다면:

```sql
FROM DATAKNOWS.PUBLIC.REGION_APT_RICHGO_MARKET_PRICE_M_H
```

지금은 실제로 이렇게 바꾸는 식입니다.

```sql
FROM HACKATHON_2026.REGION_APT_RICHGO_MARKET_PRICE_M_H
```

즉, 문서의 사고방식은 그대로 쓰고, DB 경로만 현실에 맞게 교정하면 됩니다.

---

## 10. 지금 제일 중요한 판단: EMD 유지 여부

법정동(EMD) 단위로 anomaly detection을 돌리려면 데이터 포인트가 충분해야 합니다.

먼저 아래를 확인합니다.

```sql
SELECT SGG, EMD, COUNT(DISTINCT YYYYMMDD) AS data_points
FROM HACKATHON_2026.REGION_APT_RICHGO_MARKET_PRICE_M_H
WHERE SGG IN ('서초구', '영등포구', '중구')
  AND REGION_LEVEL = 'emd'
GROUP BY 1, 2
ORDER BY 3;
```

판단 기준:

- 대부분 지역이 24개 이상이다 → EMD 유지
- 부족한 지역이 많다 → SGG로 올리는 fallback 고려

이건 나중 문제가 아니라 **지금 바로 정해야 하는 프로젝트 grain 결정**입니다.

---

## 11. 그다음 꼭 해야 하는 것: 조인 키 검증

설계상 리스크가 가장 큰 부분 중 하나가 조인 키 불일치입니다.

특히 확인 대상:

- `M_SCCO_MST`의 지역 코드/지역명
- `V01`의 설치 지역 텍스트
- 시세 테이블의 `SGG`, `EMD`
- 유동인구 테이블의 `CITY_CODE`, `DISTRICT_CODE`

이유:

- 이름 기반 조인만 믿으면 나중에 누락 발생 가능
- 서울 3개 구만 쓴다고 해도 값 표기 차이가 있으면 조인 깨질 수 있음

먼저 할 일:

1. `M_SCCO_MST` 구조 확인
2. `V01` 구조 확인
3. 지역명 표기 방식 비교
4. 필요시 매핑용 dictionary/view 추가

---

## 12. 지오메트리 컬럼도 초반에 확인

Streamlit 지도에서 쓰려면 `DISTRICT_GEOM` 같은 컬럼이 실제로 있고, 어떤 타입인지 먼저 확인해야 합니다.

예시:

```sql
DESC TABLE HACKATHON_2026.M_SCCO_MST;
```

또는 실제 샘플 테스트:

```sql
SELECT ST_ASGEOJSON(DISTRICT_GEOM)
FROM HACKATHON_2026.M_SCCO_MST
LIMIT 5;
```

이게 초반에 되는지 봐야 나중에 지도 단계에서 안 막힙니다.

---

## 13. 1차 EDA가 끝난 뒤 진행 순서

### 13-1. price_timeseries_view 만들기

목적:

- anomaly detection용 시세 시계열 입력 준비

체크 포인트:

- `TS`는 DATE 타입
- `REGION_KEY` 규칙 일관성
- EMD 또는 SGG 기준 통일

### 13-2. pop_timeseries_view 만들기

목적:

- anomaly detection용 유동인구 시계열 입력 준비

체크 포인트:

- 월 단위 날짜 변환 정상 여부
- 코드 조인 정상 여부

### 13-3. customer_profile_view 만들기

목적:

- 연령대/소득 구간 프로파일 생성

체크 포인트:

- 컬럼명 실제 존재 여부
- 기준 월 선택 가능 여부

---

## 14. 그다음 모델 단계

전처리 뷰 확인이 끝나면:

1. 시세 anomaly model 생성
2. 유동인구 anomaly model 생성
3. 2024 이후 구간 탐지 실행
4. 결과 테이블 생성

그 뒤:

- `REGION_ALERTS` 생성
- `MARKETING_ALERTS` 생성
- Cortex COMPLETE로 마케팅 문구 생성
- Streamlit 연결

---

## 15. 지금 기준으로 가장 현실적인 작업 우선순위

### 오늘 바로

1. Marketplace에서 필요한 데이터셋 추가 완료
2. `HACKATHON_2026` 실제 테이블 목록 확인
3. 시세 테이블 EDA 실행
4. 유동인구 테이블 존재 여부 확인
5. `M_SCCO_MST` 존재 여부 확인
6. 법정동 포인트 수 확인
7. EMD 유지/SGG fallback 결정

### 내일 전까지

8. 조인 키 검증
9. 지오메트리 타입 확인
10. 전처리 뷰 3개 초안 작성

### 그다음

11. anomaly detection 모델 생성
12. alert 테이블 생성
13. marketing copy 생성
14. Streamlit 데모 구성

---

## 16. 네가 지금 헷갈리면 안 되는 포인트

### 16-1. Warehouse와 데이터셋은 다른 문제다

- Warehouse: 쿼리를 돌리는 컴퓨트
- Marketplace 데이터셋: 조회할 실제 데이터

`USE WAREHOUSE`가 된다고 해서 데이터 접근이 자동 해결되는 건 아닙니다.

### 16-2. 설계 문서 경로와 실제 경로가 다를 수 있다

문서는 초안 설계이기 때문에 실제 Snowflake에 붙은 데이터 경로와 다를 수 있습니다.

그래서 지금은:

- 문서의 로직은 유지
- 실제 DB/테이블명은 Snowflake에서 확인한 값으로 교체

이 방식이 맞습니다.

### 16-3. 예시 worksheet가 열렸다는 건 오히려 좋은 신호다

이건 대개

- 데이터셋 연결 성공
- 공급자 샘플 쿼리 제공
- 네가 바로 탐색 시작 가능

이라는 뜻입니다.

---

## 17. 추천 체크리스트

아래 항목을 하나씩 체크하면서 가면 됩니다.

- [ ] `COMPUTE_WH` 선택 완료
- [ ] Marketplace 데이터셋 Get 완료
- [ ] `SHOW DATABASES` 확인 완료
- [ ] `HACKATHON_2026` schema/table 목록 확인 완료
- [ ] 시세 테이블 첫 조회 성공
- [ ] 서울 3개 구 count/date range 확인 완료
- [ ] 법정동 포인트 수 확인 완료
- [ ] 유동인구 테이블 위치 확인 완료
- [ ] `M_SCCO_MST` 위치 확인 완료
- [ ] 조인 키 검증 시작
- [ ] geometry 타입 확인
- [ ] 전처리 view 설계 시작

---

## 18. 바로 다음에 할 추천 SQL

```sql
USE WAREHOUSE COMPUTE_WH;
SHOW DATABASES;
SHOW SCHEMAS IN DATABASE HACKATHON_2026;
SHOW TABLES IN DATABASE HACKATHON_2026;

SELECT COUNT(*), MIN(YYYYMMDD::DATE), MAX(YYYYMMDD::DATE)
FROM HACKATHON_2026.REGION_APT_RICHGO_MARKET_PRICE_M_H
WHERE SGG IN ('서초구', '영등포구', '중구');

SELECT SGG, EMD, COUNT(DISTINCT YYYYMMDD) AS data_points
FROM HACKATHON_2026.REGION_APT_RICHGO_MARKET_PRICE_M_H
WHERE SGG IN ('서초구', '영등포구', '중구')
  AND REGION_LEVEL = 'emd'
GROUP BY 1, 2
ORDER BY 3;
```

이 결과가 나오면, 그다음부터는 `design.md`의 전처리/모델 단계로 넘어가면 됩니다.

---

## 19. 한 줄 결론

지금 네가 해야 하는 건

**Marketplace에서 데이터를 붙였다면, 이제 예시 worksheet에 나온 실제 DB 기준으로 테이블을 확인하고, design 문서의 EDA 쿼리를 그 실제 경로로 바꿔 실행하는 것**입니다.

문서의 방향은 맞고, 지금부터는 **실제 Snowflake 환경에 맞는 테이블명/경로 매핑 작업**이 핵심입니다.
