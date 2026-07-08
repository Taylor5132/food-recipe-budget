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

## 기술 스택 (확정)

**단일 언어(Python) 백엔드: FastAPI API + ML + 데이터 파이프라인** (폴리글랏 세금 없음)

| 레이어 | 스택 |
|---|---|
| Frontend | React + Vite + TypeScript, PWA, TanStack Query·Zustand, Tailwind |
| Backend API | **FastAPI** (FastAPI Gateway, PyJWT, SQLAlchemy+Alembic, confluent-kafka, Pydantic) |
| ML 서빙 | **FastAPI** 통합 pod (API와 동일 언어 → 코드 공유) |
| 데이터 파이프라인 | **Python** (confluent-kafka 크롤러/폴러/컨슈머) |
| 저장소 | **PostgreSQL**(OLTP) + **ClickHouse**(가격 시계열·시세예측) + Elasticsearch(레시피) + Redis |
| ML | CRF(sklearn-crfsuite)·XGBoost·LightGBM(시세예측 회귀) — 전부 CPU / Argo Workflows + MLflow |
| 🐳 Docker | 멀티스테이지, 프론트 nginx:alpine, Trivy, **Harbor**(개발 레지스트리) |
| ☸️ Kubernetes (클라우드 무관) | kubeadm · FastAPI Gateway · HPA+KEDA · Strimzi/CNPG/Altinity · cert-manager · ArgoCD+Argo Workflows · kube-prometheus+Loki+Tempo+OTel · Sealed Secrets·local-path·MetalLB(개발) |
| ☁️ AWS (마이그레이션) | EC2 · CCM · Karpenter · NLB · EBS · S3 · ECR · ESO+Secrets Manager · Route53+ExternalDNS · IRSA · Terraform · GitHub Actions |
| 🟡 보류 | CNI+메쉬(Cilium 유력) · Gateway API 구현체 · Kyverno·Velero |

> 원칙: Docker·K8s = 클라우드 무관(로컬 kubeadm 개발) / AWS = 마이그레이션 계층. 상세 = `docs/design.md` §6.7–6.8

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

> **가격 전략:** 가격 지능은 **신선식품**(KAMIS/전국도매시장 — 합법·변동큼·예측가치↑)에 집중. 가공식품 시장가 합법소스는 부재(참가격 폐기·네이버 종료) → **유저 영수증 OCR**로 개인 실구매가 추적.

| 소스 | 역할 | 검증 (2026-07-08) |
|---|---|---|
| **전국 32개 공영도매시장 실시간경매** (15141808 · `B552845/katRealTime2/trades2`) | ⭐ hero 실시간 스트림 | ✅ **실호출 200 정상** |
| **일별 도소매 가격** (`B552845/perDay/price`) | #9 신선식품 시세 근간 | ✅ **실데이터**(쌀 서울 중도매 59,400원, 전체필드) |
| **KAMIS** (kamis.or.kr) | 신선 소매 시세 | ✅ 라이브 실증 |
| **식약처 COOKRCP01** (15060073) | 레시피 + NER | ✅ sample 실증 (실키 발급 예정) |
| 농교원 레시피 기본/재료 | 레시피 콘텐츠 | 🔵 data.mafra.go.kr 등록·검증 예정 |
| 농식품올바로 (rda) / KADX | 레시피 콘텐츠 | ❓ 검증 예정 |
| 식품 회수/판매중단 (15074318) | ⚡ 안전알림 + 스파이크 | ✅ 스펙 확인 |
| 기상청 단기예보 (15084084) | 시세예측 ML 피처 | ✅ 스펙 확인 |
| 국가데이터처 온라인가격 (15080757) | 가공식품 참조(P1) | ✅ 실존 |
| KOGL 향토음식 | 레시피 크롤(요구사항) | 크롤 트랙 |
| 유저 영수증 OCR (#6) | 가공식품 개인 실구매가 | 본인 데이터(합법) |

**❌ 죽음/제외:** 참가격(3043385, API 폐기 6/6 404) · 메뉴젠(15081047, 404) · 네이버 쇼핑 API(2026-07-31 종료) · 쿠팡/SSG/롯데(소비자 API 없음) · 가락8초·위키·NEIS(적합성 감사 드롭)

**영상 레시피 추출(#2):** MVP=YouTube만 (Data API 설명란→NER). 틱톡/릴스 후속. 유저 건별·개인용·비저장.

> 상세 검증 로그: [docs/design.md §2.5](docs/design.md)

## 팀

5명 / 8-9주 풀타임

## 설계 문서

[docs/design.md](docs/design.md) — 설계 정본 (소스오브트루스)
