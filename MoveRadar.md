---
type: project
status: in-progress
created: 2026-03-31
updated: 2026-03-31
due: 2026-04-12
tags: [snowflake, hackathon, cortex, marketing, AI]
---

# MoveRadar

## 목표 (Outcome)
> 이 프로젝트가 끝났을 때 무엇이 달라져 있는가?

아파트 시세 급등·유동인구 이동 신호를 Snowflake Cortex로 자동 탐지하고, 탐지된 지역·고객 프로파일 기반 마케팅 문구를 LLM이 자동 생성하는 파이프라인을 완성해 **Snowflake KR Hackathon 2026에 제출한다.**

## 완료 기준 (Definition of Done)
- [ ] Snowflake 환경 세팅 및 데이터셋 접근 확인
- [ ] Snowpark 데이터 파이프라인 구축 (시세 + 유동인구 + 계약 데이터 결합)
- [ ] Cortex Anomaly Detection — 시세·유동인구 이상 급등 지역 탐지 구현
- [ ] Cortex COMPLETE (LLM) — 지역·고객 프로파일 기반 마케팅 문구 자동 생성 구현
- [ ] Streamlit 대시보드 구축 (이사 수요 경보 지도 + 문구 미리보기)
- [ ] 데모 녹화 영상 완성 (10분 이내, 코드·모델·사용법·인사이트 포함)
- [ ] 과제 템플릿 작성 및 소스 코드 zip 준비
- [ ] 과제 제출 Forms 제출 (마감: 2026-04-12)

## 범위
- **포함:** Snowflake Cortex 기반 이상 탐지, LLM 문구 생성, Streamlit 대시보드, 데모 영상
- **제외:** 실제 마케팅 발송 시스템 연동, 캐릭터 일관성 유지 기능, 외부 API 연동

## 마일스톤
| 날짜 | 내용 |
|------|------|
| 2026-03-31 | 프로젝트 킥오프 |
| 2026-04-04 | 데이터 탐색 완료 + Snowpark 파이프라인 구축 |
| 2026-04-08 | Cortex Anomaly Detection + LLM 문구 생성 구현 완료 |
| 2026-04-10 | Streamlit 대시보드 완성 |
| 2026-04-11 | 데모 영상 녹화 + 과제 템플릿 작성 |
| 2026-04-12 | 최종 제출 (마감) |

## 현재 작업 (Next Actions)
- [ ] Snowflake 계정 및 데이터셋 접근 권한 확인
- [ ] 제공 데이터셋 EDA (REGION_APT_RICHGO, FLOATING_POPULATION_INFO, V01, V04)
- [ ] Cortex Anomaly Detection 기능 테스트

## 리스크
- 데이터 결합 복잡도 높음 — 12일 내 완성도 확보가 관건
- Cortex 기능 사용 한도(쿼터) 확인 필요
- 아파트 시세 선행 신호와 계약 데이터 간 시차 검증 필요

## 진행 로그

### 2026-03-31
- 프로젝트 킥오프
- Literature 노트 기반으로 아이디어 2(이사 시즌 선제 마케팅 인텔리전스) 채택

## 참조
- [[5-Zettelkasten/10. Literature/2026-03-31-이사시즌-선제마케팅-인텔리전스]] — 아이디어 원천
- [[3-Resources/해커톤/SNOWFLAKE-KR-HACKATHON-2026]] — 해커톤 제출 요건 및 데이터셋
