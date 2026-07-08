# 냉장고 매니저 — 예산 기반 스마트 식단 플랫폼

냉장고 재고 관리 + 유통기한 모니터링 + **실시간 도매시장 시세** 기반 예산 밀플래닝을 하나의 루프로 연결하는 음식/요리 앱.

## 아키텍처

- **MSA** — 상시 서비스 7 (Gateway, User, Pantry, Recipe, Price, MealPlan, ML Serving) + 배치/파이프라인 5
- **Data Pipeline** — Kafka 기반 이기종 프로듀서(실시간 스트림/일별 폴/이벤트/크롤) → 다중 컨슈머
- **Custom AI** (전부 CPU 학습·서빙 — GTX 1060 3GB 제약)
  - P0: 한식 재료 NER (CRF) · 시세 예측 (시계열)
  - P1: 신선도 예측 (XGBoost) · 레시피 랭킹 (LightGBM)
- **Auth** — Kakao 소셜 + 자체 이메일/비번, 로그인 성공점에서 자체 JWT 통일
- **Infra** — kubeadm full K8s on AWS / Docker / Terraform / ArgoCD(GitOps) / Prometheus+Grafana+Loki+Tempo

## 프로젝트 구조

```
food-cooking-app/
├── docs/                      # 설계 정본 (design.md = 소스오브트루스)
├── services/                  # MSA 백엔드 서비스
│   ├── gateway/               # API 라우팅 + JWT 검증
│   ├── user-service/          # 인증(Kakao+자체) + JWT 발급
│   ├── pantry-service/        # 냉장고 재고 + 유통기한 + 안전알림
│   ├── recipe-service/        # 레시피 검색/추천 + 외부 레시피 구조화
│   ├── price-service/         # 실시간 시세 + 멀티마켓 비교
│   ├── meal-plan-service/     # 예산 밀플래닝 + 장바구니 생성
│   └── social-service/        # [P1] 피드 공유 + 아카이빙
├── ml/                        # 커스텀 AI 모델 (전부 CPU)
│   ├── ingredient-ner/        # [P0] 한식 재료 NER (CRF)
│   ├── price-forecaster/      # [P0] 시세 예측 (실시간 경락가→소매가)
│   ├── freshness-predictor/   # [P1] 신선도 예측 (XGBoost)
│   ├── recipe-ranker/         # [P1] 레시피 랭킹 (LightGBM)
│   └── food-classifier/       # [P1/보류] 식재료 이미지 분류 (원재료 데이터 부족)
├── data-pipeline/             # Kafka 데이터 파이프라인
│   ├── crawlers/              # KOGL 향토음식(벌크) / 위키 EventStreams(폴백)
│   ├── pollers/               # 32시장 실시간경매, NEIS, KAMIS, 참가격, 온라인가격, 식품회수, 기상청
│   └── kafka/                 # 토픽 설정, Avro 스키마
└── infra/                     # 인프라 코드
    ├── k8s/
    ├── terraform/
    └── docker/
```

## 데이터 소스 (전부 합법 — 공공 오픈데이터/공식 API. 사설 플랫폼 크롤 금지)

| 소스 | 유형 | 갱신 | 검증 |
|---|---|---|---|
| **전국 32개 공영도매시장 실시간 경매** (15141808) | 공공 API ⭐ hero | **실시간 (장중 연속)** | ✅ 실존·자동승인 |
| 가락시장 8초 피드 (15004517) | 공공 API | ~실시간 | ✅ 실존 (선택) |
| KAMIS 일별 도·소매가격 (15156057) | 공공 API | 일별 | ✅ 실존·자동승인 |
| 한국소비자원 참가격 (3043385) | 공공 API | 격주 | 🔴 마트명·가격 필드 키확인 필요 |
| 국가데이터처 온라인 수집가격 (15080757) | 공공 API | 일별 | ✅ 실존·자동승인 |
| 식약처 레시피 COOKRCP01 (15060073) | 공공 API | — | ✅ 라이브 실증 |
| 농교원 레시피 기본/재료 (15057205/15058981) | 공공 API | — | ✅ 실존 (재료 분량·단위 키확인) |
| NEIS 학교급식 식단 (15139198) | 공공 API | 일별 대량 | ✅ 라이브 실증(무키) |
| 식품 회수/판매중단 (15074318) | 공공 API | 이벤트 | ✅ 실존·필드완전 |
| 기상청 단기예보 (15084084) | 공공 API | 시간별 | ✅ 실존 (ML 피처) |
| KOGL 향토음식 / 위키 EventStreams | 크롤 (KOGL·CC) | 벌크/연속 | 크롤링 요구 충족 |

> 상세 검증 로그: [docs/design.md §2.5](docs/design.md)

## 팀

5명 / 8-9주 풀타임

## 설계 문서

[docs/design.md](docs/design.md) — 설계 정본 (소스오브트루스)
