# CLAUDE.md — 냉장고 매니저 (food-recipe-budget)

작업 전 필독. **설계 정본 = `docs/design.md`** (소스오브트루스, 재파생 금지).

## 프로젝트
냉장고 재고 + 유통기한 + 실시간 도매시장 시세 기반 예산 밀플래닝 웹앱.
AI 해커톤 + 인프라 캡스톤 겸용 (5인, 8-9주).

## 절대 제약
- **AI 전부 CPU** (GTX 1060 3GB → GPU 학습 불가). CRF/XGBoost/LightGBM/시계열만.
- **학생 예산** — GPU 인스턴스 금지, AWS Spot+셀프호스트.
- **데이터 합법만** — 공공 오픈데이터/공식 API/CC·KOGL만.
  사설 플랫폼 크롤 절대 금지 (쿠팡/네이버/SSG/블로그 = 야놀자 형사판례·DB권).
  AI 학습 목적도 불가 (한국 TDM 면책 미통과).

## 기술 스택 (확정)
**단일 언어(Python): FastAPI API + ML + 데이터 파이프라인.**
PG(OLTP) + ClickHouse(가격 시계열/시세예측) + Elasticsearch(레시피) + Redis.
Kafka(Strimzi) + KEDA. kubeadm on AWS, Terraform, GitHub Actions+ECR+ArgoCD, LGTM.
프론트=React/Vite/PWA. Docker 멀티스테이지(프론트=nginx 정적 서빙). → 상세 §6.7

## 커스텀 AI (ChatGPT-moat, 전부 CPU)
- P0: 한식 재료 NER(CRF) · 시세 예측(시계열)
- P1: 신선도 예측(XGBoost) · 레시피 랭킹(LightGBM)

## 데이터소스 (2026-07-08 실검증)
- ✅ 살아있음: 전국32시장 실시간경매(`B552845/katRealTime2/trades2`), 일별도소매(`B552845/perDay/price`), KAMIS, 식약처 COOKRCP01
- ❌ 죽음/제외: 참가격(폐기), 메뉴젠(404), 네이버쇼핑(7/31 종료), 쿠팡/SSG/롯데(API없음)
- 키: data.go.kr 일반인증키(aT 가격 API) / 식약처·mafra는 별도 포털. → 상세 §2.5

## 작업 규칙 (중요)
- **문서 수정 전 물어보고, 확정된 것만 기록.** 내 추천을 결정처럼 쓰지 말 것.
- 학습 목적: 손으로 이해하며 — 완성품 덤프 X, 조각내 설명 먼저.
- 설계 결정: 숫자+근거로 종이 위에서. 실인프라 테스트 제안 X.

## 미정 (사용자 결정 대기 — 임의로 정하지 말 것)
- CNI + 서비스 메쉬 (Cilium 유력, 보류)
- Gateway API 구현체 (Cilium Gateway / Envoy Gateway / Traefik — CNI에 연동)
- 5인 역할분담 + 9주 타임라인
