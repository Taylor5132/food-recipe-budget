# 냉장고 매니저 — 예산 기반 스마트 식단 플랫폼

냉장고 재고 관리 + 유통기한 모니터링 + 실시간 시세 기반 예산 밀플래닝을 하나의 루프로 연결하는 음식/요리 앱.

## 아키텍처

- **MSA** — 7 서비스 (Gateway, User, Pantry, Recipe, Price, MealPlan, Social)
- **Data Pipeline** — Kafka 기반 이기종 크롤러/폴러 → 다중 컨슈머
- **Custom AI** — 한식 재료 NER, 식재료 이미지 분류, 신선도 예측
- **Infra** — K8s / Docker / AWS / Terraform

## 프로젝트 구조

```
food-cooking-app/
├── docs/                      # 설계 문서
├── services/                  # MSA 백엔드 서비스
│   ├── gateway/
│   ├── user-service/
│   ├── pantry-service/        # 냉장고 데이터 + 유통기한
│   ├── recipe-service/        # 레시피 엔진 + 추천
│   ├── price-service/         # 가격 시세 + 마트별 비교
│   ├── meal-plan-service/     # 예산 밀플래닝 + 장바구니
│   └── social-service/        # [P1] 피드 공유 + 아카이빙
├── ml/                        # 커스텀 AI 모델
│   ├── ingredient-ner/        # 한식 재료 NER
│   ├── food-classifier/       # 식재료 이미지 분류
│   └── freshness-predictor/   # 신선도 예측
├── data-pipeline/             # Kafka 데이터 파이프라인
│   ├── crawlers/              # 농식품올바로, 식약처 크롤러
│   ├── pollers/               # KAMIS, 참가격, 국가데이터처 API 폴러
│   └── kafka/                 # 토픽 설정, Avro 스키마
└── infra/                     # 인프라 코드
    ├── k8s/
    ├── terraform/
    └── docker/
```

## 데이터 소스

| 소스 | 유형 | 커버리지 |
|---|---|---|
| KAMIS (농수산물유통정보) | 공공 API | 신선식품 일별 시세 |
| 한국소비자원 참가격 | 공공 API | 가공식품 매장별 실제 판매가 |
| 국가데이터처 온라인 수집 가격 | 공공 API | 온라인몰 일별 가격 |
| 농식품올바로 (cookrg.kr) | 공공 데이터 | 레시피 + 재료 + 영양정보 |
| 식약처 레시피 DB | 공공 데이터 | 레시피 + 유통기한 가이드라인 |

## 팀

5명 / 8-9주 풀타임

## 설계 문서

[docs/design.md](docs/design.md) — 설계 정본 (소스오브트루스)
