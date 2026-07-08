# 음식/요리 앱 설계 정본

> **이 문서가 소스오브트루스.** grilling 세션에서 확정된 결정사항을 실시간 반영.
> 최종 수정: 2026-07-08

---

## §0. 프로젝트 컨텍스트

- **정체성:** AI 해커톤 + 인프라 캡스톤 **하나의 프로젝트**로 양쪽 평가 기준 모두 충족
- **팀:** 5명, 8-9주, 풀타임
- **타겟:** B2C (소비자 대상)
- **인프라 요구사항:** K8s / Docker / AWS, Kafka 데이터 파이프라인, MSA 구조
- **AI 요구사항:** 커스텀 학습 AI (ChatGPT-moat — 상용 모델이 못하는 영역에서만 정당화)
- **데이터 원칙:** 공공 오픈데이터 / 공식 API / CC 라이선스만 사용. **사설 플랫폼 크롤링 금지** (야놀자 v 여기어때 형사 판결 precedent)

### 하드웨어/예산 제약 (핵심 — AI 전략을 지배함)

| 리소스 | 보유 | 함의 |
|---|---|---|
| GPU | GTX 1060 **3GB VRAM** | Transformer(KoBERT 4-8GB) / CNN fine-tune **불가**. GPU 학습 배제 |
| CPU | 16코어 | CRF/XGBoost/LightGBM 학습·서빙 충분 |
| RAM | 32GB | 로컬 k3s + Kafka + ES + Redis + PG + 서비스 = 빡빡하나 가능 |
| SSD | 1TB | 충분 |
| 예산 | 학생 (최소화 필수) | **GPU 인스턴스(g4dn=월 $380) 배제.** AWS는 CPU-only |

**결론: 모든 커스텀 AI는 CPU에서 학습+서빙 가능해야 함. GPU 워크로드 완전 제거.**

---

## §1. 서비스 목록 및 우선순위

### P0 — 핵심 루프

| # | 서비스 | 설명 | AI 유형 |
|---|---|---|---|
| 6 | 냉장고 데이터 저장 | 쇼핑몰 결제 완료창 OCR + 실제 음식 사진으로 냉장고 재고 관리 | 커스텀 AI (식재료 이미지 분류) |
| 3 | 유통기한/신선도 모니터링 | 식재료별 보관법·온도 기반 남은 신선도 예측 + 알림 | 커스텀 AI (신선도 예측 모델) |
| 4 | 남은 음식 기반 레시피 | 냉장고 재고 + 유통기한 우선순위로 레시피 추천 | 매칭/랭킹 알고리즘 |
| 1 | 월 식비 예산 밀플래닝 | 실시간 시세 + 냉장고 재고 + 예산 상한 → 주간 식단 최적화 | 제약 조건 최적화 + 유저 선호 학습 |
| 8 | 장바구니 리스트 생성 | 레시피 재료 - 냉장고 재고 = 필요 구매 목록 | 규칙 기반 (diff) |
| 9 | 장바구니 예상 가격 계산 (재구성) | **지역 평균 소매가(KAMIS) 대비 비교** (P0) + 온라인 최저가(온라인가격/네이버쇼핑, P1). ~~매장명별 비교~~는 합법소스(참가격) 폐기로 불가 | 데이터 조회 (AI 불필요) |
| 13 | 오늘의 식재료 시세 추천 | KAMIS 시세 기반 "오늘 싼 재료"로 레시피 추천 | 데이터 + 규칙 |
| 14 | ⚡ 냉장고 안전 알림 (신규) | 식품 회수/판매중단 발생 → 내 냉장고 매칭 → 알림 | 매칭 규칙 + Kafka fan-out |

### P0 인프라

| # | 서비스 | 설명 | AI 유형 |
|---|---|---|---|
| 2 | 외부 레시피 추출 | URL/사진 → 레시피 구조화. **영상: MVP=YouTube만** (Data API로 설명란→NER). 사진=범용 OCR. 틱톡/릴스는 후속(P1, 텍스트 붙여넣기 방식) | 범용 Vision/OCR + NER 재사용 |
| 7 | 레시피 크롤링 → DB | 합법 소스 크롤링 + 전처리 + 재료 추출 → 레시피 DB | 커스텀 AI (한식 재료 NER) |

### P1 — 확장 (MVP 이후)

| # | 서비스 | 설명 |
|---|---|---|
| 11 | 요리 피드 공유 | 소셜 기능 (집밥/요리 사진 피드) |
| 5 | 다중 요리 병렬 도우미 | 여러 요리 동시 조리 시 통합 타임라인 생성 |
| 10 | 외국 식재료 로컬라이징 | 외국 레시피의 식재료를 한국 대체재로 변환 + 레시피 생성 |
| 12 | 식사 기록 아카이빙 | 식사 사진/맛평가/가격 기록 → 유저 데이터 → 밀플래닝 피드백 |

---

## §2. 데이터 소스 및 합법성

### 가격 데이터 (P0 서비스 #1, #9, #13)

| 소스 | API 위치 | 제공 데이터 | 주기 | 커버리지 | 판정 |
|---|---|---|---|---|---|
| **★★ 전국 32개 공영도매시장 실시간 경매정보** | data.go.kr/15141808 (aT) | 32개 시장 경매 낙찰가 (품목/거래량/가격/산지) | **실시간 (장중 연속)** | 전국 신선식품 도매 (가락·강서·구리·대구·부산·광주·대전·인천 + 수산 공판장) | ✅ 합법, **hero 실시간 스트림 (단일 통합점)** |
| ~~가락시장 8초 (15004517)~~ | — | 가락 단독 고해상도 | — | — | ❌ **드롭** (전국 피드의 부분집합, 쓰는 기능 없음) |
| **★ KAMIS** (kamis.or.kr 자체 + data.go.kr 15156062 지역별/15156057 일별) | `service/price/xml.do?action=dailySalesList\|dailyPriceByCategoryList` | 농수산물 도소매 시세 (지역/등급별 최저·평균·최고가) | 일별 | 신선식품 (채소/과일/육류/수산/곡류) | ✅ **라이브 실증(2026-07-08 실데이터)** — 소매 실구매가 baseline, #9 P0 근간 |
| ~~한국소비자원 참가격 (3043385)~~ | openapi.price.go.kr | 매장명별 실판매가 | 격주 | — | ❌ **DEAD (2026-07-08 실검증)** — `/openApiImpl/ProductPriceInfoService/` 서블릿 폐기, 6/6 변형 404. data.go.kr 등록은 stale. **매장명별 비교 합법소스 부재 확정** |
| 국가데이터처 온라인 수집 가격 (15080757) | data.go.kr/15080757 | ~120품목 온라인몰 가격 (정부 직접 수집) | 일별 | 온라인몰 가격 | 🟡 **P1 강등** (4번째 가격소스, 9주 과함) |

**제외된 소스 (불법/제한):**
- 쿠팡 (재확인 2026-07-08): **소비자용 가격 API 없음.** ① Open API(WING)=셀러 전용(자기 상품 CRUD, sellerProductId) ② Partners(어필리에이트)=시간당 10콜+가격비교 ToS 위반. 2026-05 업데이트도 셀러 Auto Pricing 중심
- SSG `eapi.ssgadm.com`: 입점 셀러 전용 API (상품 등록/정산용), 소비자 검색 endpoint 없음
- 롯데/홈플러스: 공개 API 없음
- **쿠팡/네이버 직접 크롤 = 금지** (쿠팡=야놀자 형사판례, 네이버=ToS/robots.txt). "비상/개인용"도 정보통신망법·DB권·ToS는 무관 → 안 함

**❌ 네이버 쇼핑 검색 API — 2026-07-31 서비스 종료 (사용자 제보)**
- 3주 뒤 종료 → P0로 못 씀. (종료 범위가 검색 shop 엔드포인트인지 커머스/어필리에이트인지 최종 확인 필요, developers.naver.com 로그인 뒤 공지)
- 참가격(오프라인)+네이버(온라인) 연쇄 죽음 → **가공식품 시장가격 합법 소스 사실상 부재**

**★ 전략적 재구성 (2026-07-08): 가격 지능 = 신선식품 스토리**
- **신선식품(KAMIS+전국32시장)** = 합법·라이브실증·변동큼 → 예측가치 큼(시세예측AI/"지금 사? 기다려?"). **여기가 스타, 여기 집중.**
- **가공식품** = 가격 안정적(예측가치 낮음)+합법데이터 빈약 → **시장비교 API 안 씀.** 대신 **유저 본인 영수증 OCR(#6)**로 실구매가 확보(본인데이터=합법) → 개인 예산추적(#12). 시장 참조는 국가데이터처 120품목(15080757)으로 최소.
- 후보(미확인): 카카오쇼핑 Open API — 셀러전용 함정 가능성, 확인 대기

### 레시피 데이터 (P0 서비스 #2, #7) — API(정형) + 크롤(비정형) 이원화

**API 소스 (정형, 폴링) → `raw.recipes.api`** — ⏳ **레시피 소스 검증 예정 목록 (2026-07-08)**
| 소스 | 유형 | 키/등록 | 검증상태 |
|---|---|---|---|
| **식약처 COOKRCP01** (15060073) | ~1,100건, 재료 자유텍스트 | foodsafetykorea 무료·즉시 (`sample`키=샘플5건) | ✅ sample 라이브 / **실키 발급 예정** |
| **농교원 레시피 재료정보** (15058981) | 재료명+분량 structured | **data.mafra.go.kr 가입**(LINK형) | 🔵 살아있음 / **mafra 등록·검증 예정** |
| **농교원 레시피 기본정보** (15057205) | 요리명/시간/난이도/분류 | data.mafra.go.kr | 🔵 / **검증 예정** |
| **농식품올바로** (koreanfood.rda.go.kr/kfi/openapi) | 농진청 레시피+식재료 | 자체 포털 | ❓ **미검증 / 검증 예정** |
| KADX 무료 레시피 데이터 (kadx.co.kr) | 벌크 레시피 | 거래소 | ❓ **미검증 / 검증 예정** |
| ~~농진청 메뉴젠 (15081047)~~ | — | — | ❌ 404(잘못된 ID) 드롭 |
> **MVP 최소경로:** 식약처 실키(무료·즉시, 유일 검증됨)로 레시피 언블락 → 볼륨은 농교원/농식품올바로로 후속 보강. 나머지는 위 순서로 검증 예정.

**크롤/폴 소스 — 갱신 주기로 정직하게 3계층 분리 (2026-07-08 검증)**

*① 배치 레그 (정적, 일회성 벌크 + 분기 갱신) → `raw.recipes.crawled.bulk`*
| 소스 | 라이선스 | 갱신 | 얻는 것 |
|---|---|---|---|
| 한국관광공사 향토음식 웹 | KOGL | 거의 정적 | 지역 향토레시피 |
| 문화포털/지자체 향토음식 | KOGL | 월간, 소량 | 전통 레시피 |
| 농사로 제철 식재료 | 공공누리 | 계절 | 제철 정보, 손질법 |
> 이들은 **스트림 아님** — 일회성 벌크 크롤 + 주기적 refresh 배치. (한국어학습 플랫폼의 "정적 AIHub=배치"에 해당)

*② ★ 진짜 실시간 스트림 (hero) → `raw.prices.market.stream`* ⭐⭐⭐ **Kafka 스트리밍의 진짜 근거**
| 소스 | 라이선스 | 갱신 | 얻는 것 |
|---|---|---|---|
| **★★ 전국 32개 공영도매시장 실시간 경매정보** (data.go.kr/15141808, aT) | 공공 | **실시간 (장중 연속, 32개 시장)** | 시장/품목/거래량/가격/산지. 단일 API로 전국 커버 |
> 도매 경락가 = 내일 소매가 선행지표 → "지금 사? 기다려?" 시세추천 + **멀티마켓 비교("가락 vs 구리 vs 강서 어디가 싼가")** + 예산 밀플래닝 핵심.
> **32개 시장 × 장중 연속 = 진짜 대량 스트림** → Kafka 파티셔닝(시장별/품목별) 설계 명분.
> ⚠️ 단서: "실시간"=각 시장 경매시간(새벽~오전) 중 계속 게시. 표준데이터(15029181)는 일별집계라 혼동 금지. production 티어 승급은 use-case 등록 필요.
> ~~가락 8초(15004517)~~ **드롭** — 전국 피드의 부분집합. 8초를 쓰는 기능 없음(감사 §2.6).

*③ 이벤트 스트림 → `events.food.recall`*
| 소스 | 갱신 | 얻는 것 |
|---|---|---|
| **식품안전나라 회수/판매중단** (data.go.kr/15074318) + 수입식품 부적합(15095378) | 이벤트 (주 수건, 버스티) | 제품명/업체/유통기한/회수사유/바코드/분류 → ⚡ 냉장고 안전알림 + 스파이크 소스 |

*④ ML 피처 폴*
| 소스 | 갱신 | 용도 |
|---|---|---|
| 기상청 초단기실황 (data.go.kr/15084084) | 시간별 | 날씨→시세 예측 피처 (헤드라인 아님) |

*⑤ 고빈도 API 폴 → `raw.*.api`*
| 소스 | 갱신 | 얻는 것 |
|---|---|---|
| KAMIS 일별 도·소매가격 (15156057) | 일별 | 신선식품 소매 실구매가 baseline |
| 참가격 (3043385) | 격주 | 마트별 실판매가 (마트비교) |
| ~~국가데이터처 온라인가격 (15080757)~~ → **P1 강등** | 일별 | 온라인몰 채널 (4번째 가격소스는 9주 과함) |

> ~~NEIS 학교급식(15139198)~~ **드롭** — 볼륨 명분은 전국시장이 대체, 요리명만 있어 NER gold label 아님(감사 §2.6).
> ~~위키 EventStreams 폴백~~ **드롭** — 전국시장 "실시간" 확인 + 크롤은 KOGL 향토음식이 담당. 순수 명분용이라 불필요.

**불법 (사용 금지):** 네이버 블로그/유튜브/만개의레시피 크롤링 (야놀자·잡코리아 판례). AI 학습 목적도 한국은 TDM 면책 미통과라 불법.

> **핵심 구조 (정직한 축 분리):**
> - **학습 데이터** = 정형 API (농교원 재료 gold label) + 식약처 테이블 + 유저로그. 크롤 아님.
> - **실시간 스트림** = 전국 도매시장 경락가(가격축). 학습 아니라 런타임 시세.
> - **크롤(런타임 콘텐츠)** = 향토음식 벌크(크롤 요구사항 충족). 학습된 NER이 처리할 입력 공급 (load-bearing).
> - "크롤=학습" 등식 폐기 → 만개의레시피 유혹의 근원 제거.

### §2.5 데이터소스 실검증 로그 (2026-07-08)

**✅ 라이브 실증 (실제 데이터 확인)**
| 소스 | 근거 |
|---|---|
| 식약처 COOKRCP01 | `sample`키 호출 성공. RCP_PARTS_DTLS=자유텍스트("연두부 75g(3/4모)...") NER 대상 확인. total_count 존재 |
| NEIS 급식 | INFO-000 정상. DDISH_NM/ORPLC_INFO/CAL_INFO/NTR_INFO 실데이터(무키 호출) |
| **★ KAMIS (kamis.or.kr)** | `dailySalesList` 호출 → `error_code:000` + **2026-07-08 실데이터**(쌀20kg 61,502원 등 300+품목). 엔드포인트 생존 확정 |
| **★★ 일별 도소매 perDay/price (B552845, 정식키)** | ✅ **HTTP 200 정상 + 실데이터/전체필드 검증.** 엔드포인트 `apis.data.go.kr/B552845/perDay/price`. 예: 쌀 서울 중도매 exmn_dd_prc=59,400원, kg환산 2,970원. **필드: item_cd/nm, vrty, ctgry, grd, se(도소매), sgg(지역), mrkt, unit, exmn_ymd, exmn_dd_prc, exmn_dd_cnvs_prc.** #9 근간 완전 확정 |
| **★★ HERO 전국32시장 katRealTime2/trades2 (정식키)** | ✅ **HTTP 200 정상** (인증 통과, 참가격과 달리 살아있음). cond 없으면 빈 결과. **키 활성 확정 — 초기 401은 전파 대기/파라미터 문제였음** |
| **API 파라미터 문법 (B552845)** | `cond[필드::EQ\|LTE\|GTE]=값` + pageNo/numOfRows/returnType(XML\|JSON)/selectable. 날짜=YYYYMMDD (cond[exmn_ymd::…]) |

**✅ 실존·활성 확인 (스펙 페이지)**
| 소스 | 승인 | 필드 근거 |
|---|---|---|
| 전국32시장 실시간경매(15141808) | 자동승인 | "실시간"+"32개 도매시장" 확인. 상세필드는 첨부 xlsx |
| 가락 8초(15004517) | — | **"업데이트 주기: 실시간" 명시(일별집계 아님).** 품목/등급/거래수량/평균가격/전일대비지수 |
| 식품회수(15074318) | — | 제품명/바코드/제조사/유통기한/회수사유/사진URL/회수등급 전부 확인 |
| KAMIS data.go.kr 프록시(15156057, aT) | 자동승인 | KAMIS와 동일 데이터의 data.go.kr 경로(serviceKey). 조사일자/품목명/구분코드(도·소매)/시군구/시장/가격. ※KAMIS 자체는 위 라이브 실증됨 |
| 온라인 수집가격(15080757, 국가데이터처) | 자동승인 | 품목코드/품목명 확인, 가격은 별도 operation(키 확인 대상) |
| 레시피 기본정보(15057205) | — | 요리명/조리시간/난이도/인분/칼로리/분류 (5,278 활용) |
| 기상청 단기예보(15084084) | — | 초단기실황 포함. T1H(기온)/RN1(강수)/WSD(풍속) |

**🔴 실존하나 핵심필드 키 확인 필요 (키 발급 후)**
| 소스 | 확인할 것 |
|---|---|
| 농교원 재료정보(15058981) | **분량·단위 분리 필드** → NER gold label 근거 |
| 온라인가격(15080757) | 가격 operation 실제 필드 → #9 온라인 최저가 |

**❌ 깨짐/폐기 — 실검증으로 검출**
| 소스 | 문제 |
|---|---|
| 소비자원 참가격(3043385) | **DEAD.** `/openApiImpl/ProductPriceInfoService/` 6/6 변형 404(모든 op·양쪽 호스트·http/https, 레거시TLS 연결됨). 서블릿 폐기, data.go.kr 등록 stale. **매장명별 비교 합법소스 부재 → #9 재구성(B)** |
| 농진청 메뉴젠(15081047) | **404 페이지 없음. fork가 잘못된 ID 제공.** 레시피는 COOKRCP01+농교원 2종으로 충분 → 메뉴젠 드롭 |

**다음 단계 — 남은 소스 실검증 (키 활성 확정됨, 같은 일반 인증키로):**
| 우선 | 소스 | 상태 | 링크 |
|---|---|---|---|
| ✅ | 전국32시장 실시간경매 15141808 (HERO) | **검증완료: `B552845/katRealTime2/trades2` 200 정상. 키 활성 확정** | data.go.kr/data/15141808 |
| ✅ | 일별 도소매 (perDay/price) | **검증완료: 200 정상 + 전체필드/실가격** (#9 근간) | data.go.kr/data/15156057 |
| 🔵 | 농교원 레시피 기본(15057205)+재료(15058981) | **레시피 콘텐츠 소스(#7 DB), NER 재료 gold label 겸용.** LINK형 → **data.mafra.go.kr 회원가입(인증키 자동발급)+Open API 사용신청**으로 접근. 참가격과 달리 살아있음 → **등록해서 쓸 가치 O**(식약처 1,100건에 레시피 양·다양성 추가). mafra 키+미리보기 URL 확보시 검증 | data.mafra.go.kr |
| 🔴 | 온라인가격 15080757 | 가격 op 필드 (#9 온라인 P1) | data.go.kr/data/15080757 |
| 🟡 | 식품회수 15074318, 기상청 15084084, 레시피기본 15057205 | 라이브 미테스트 | 각 data.go.kr/data/{id} |

KAMIS 키: [data.go.kr 15156057](https://www.data.go.kr/data/15156057/openapi.do)(프록시,즉시) 또는 [kamis.or.kr](https://www.kamis.or.kr/customer/reference/openapi_list.do)(자체). 식약처 COOKRCP01(15060073)은 sample키 라이브 실증됨(프로덕션 정식키만 추가). 참가격은 DEAD 종결.

### §2.6 데이터소스 적합성 감사 (2026-07-08) — "기능이 소비하나 vs 명분용 억지인가"

**✅ GENUINE — 실제 P0 기능이 소비**
| 소스 | 소비 기능 |
|---|---|
| 전국32시장 실시간경매 | #13 시세추천 + #1 예산 + 시세예측AI + "지금 사? 기다려?" (다중 P0) |
| KAMIS 일별 소매 | #9 가격계산 — **지역 평균 소매가 baseline (참가격 폐기 후 #9 근간)** |
| ~~참가격 (마트별)~~ | ❌ **DEAD** — API 폐기(6/6 404). 매장명별 비교 불가 → #9는 KAMIS 지역평균+온라인최저가로 재구성 |
| 식약처 COOKRCP01 | #7 레시피 DB + NER |
| 농교원 기본/재료 | #7 레시피 DB + NER gold label + #8 장바구니 |
| 식품회수 | #14 안전알림 + 스파이크 |
| 식약처 소비기한 | 신선도 AI(P1) + #3 유통기한 |

**🟡 BORDERLINE — 내용 진짜지만 부차/요구사항 주도 (유지, 정직하게)**
| 소스 | 판정 |
|---|---|
| KOGL 향토음식(벌크) | 주 목적=크롤 요구사항 충족. 단 합법 KOGL로 진짜 레시피 추가 → "가장 덜 억지스러운 크롤" |
| 기상청 | 시세예측 AI 피처로만(유저대면 아님). 진짜 피처지만 부차적 |
| 국가데이터처 온라인가격 | 온라인 채널 가격, 상보적이나 → **P1 강등** |

**🔴 SHOEHORN → 드롭**
| 소스 | 이유 |
|---|---|
| 가락 8초(15004517) | 전국 피드의 부분집합. 8초 쓰는 기능 없음 = "데모 화려하게"뿐 |
| 위키 EventStreams | 순수 "연속 크롤 명분" 폴백. 전국시장 실시간 확인 + 크롤은 KOGL 담당 → 불필요 |
| NEIS 급식 | 볼륨 명분=전국시장이 대체. 요리명만 있어 NER gold label 아님 |

> **감사 결론:** 4건 정리(가락8초·위키·NEIS 드롭 + 온라인가격 P1) 후, 남은 소스는 **전부 실제 기능이 소비.** 억지 명분 소스 제거 완료.

### AI 학습 데이터 (2026-07-08 검증 완료)

**1. 한식 재료 NER — ✅ 예상보다 좋음**
- 식약처 COOKRCP01: ~1,100건 레시피, 재료 필드 = 자유 텍스트 ("양파 1/2개, 간장 2큰술") → NER 학습 대상
- 농교원 레시피 재료정보 API (data.go.kr/15058981): **재료명+수량+단위 structured** → gold label로 직접 사용 가능
- 농교원 레시피 기본정보 API (data.go.kr/15057205): 요리명, 조리시간, 난이도, 분류 (실존 확인)
- ~~농촌진흥청 메뉴젠(15081047)~~: 404 — 드롭
- KADX 농식품 빅데이터 거래소: 전체 레시피 데이터 별도 제공
- **총 수천 건+ 확보 가능, 구조화된 gold label 존재 → NER 학습 충분**

**2. 식재료 이미지 분류 — ⚠️ 완성 요리 이미지는 풍부, 원재료 이미지는 부족**
- AIHub 건강관리용 음식 이미지 (aihub.or.kr/242): 300만 장, 500카테고리 → **완성 요리** (비빔밥, 김치찌개)
- AIHub 음식 이미지+영양 (aihub.or.kr/74): 84만 장, 400종 → **완성 요리**
- AIHub 한국 이미지-음식 (aihub.or.kr/79): 15만 장, 150종 → **완성 요리**
- AIHub 농산물 QC 이미지 (aihub.or.kr/149): 30만 장, **10개 농산물** 품질등급 → **원재료이지만 10종뿐**
- **핵심 갭: 냉장고 속 원재료(양파/당근/고추장) 분류용 대규모 데이터셋 없음**
- 대안: 농산물 QC(10종) + 완성요리 데이터셋의 재료 태그 활용, 또는 범용 Vision 모델로 대체

**3. 신선도 예측 — ⚠️ 정형 데이터 없음, 수동 파싱 필요**
- 식약처 소비기한 참고값: 1,450품목 공개, 보관온도별 실험 결과 기반
- 형태: **PDF/고시/카드뉴스** → structured CSV/JSON 아님
- 수동 파싱하여 (food_type → storage_method → shelf_life_days) 테이블 구축 필요
- 작업량: ~200-500 품목 정리 = 1-2일 수동 작업

---

## §3. 커스텀 AI 계획 (ChatGPT-moat)

### 원칙
커스텀 학습 AI는 **상용 LLM(ChatGPT/Claude)이 할 수 없는 영역**에서만 정당화:
- 실시간/현재 데이터 기반 계산
- 한국 특화 니치 데이터 (한식 계량법, 한국 식재료)
- 대규모 집계/통계 기반 추론

### 커스텀 AI 4종 (하드웨어 제약 반영 — 전부 CPU 학습+서빙)

> **우선순위: P0 = ①재료 NER + ②시세 예측 / P1 = ③신선도 + ④레시피 랭킹**
> (P0 = 파이프라인·제품가치 핵심 + Day1 학습데이터 존재 / P1 = 유저 로그 축적 필요, 콜드스타트는 규칙기반)

**① 한식 재료 NER + 정규화 모델 [P0]** ✅ 파이프라인 중심
- 목적: 레시피/OCR 텍스트에서 재료·수량·단위 정확 추출
- ChatGPT-moat: 한국 요리 비정형 계량("한줌", "적당량", "눈대중", "반큰술")과 식재료 동의어/방언("감자=하지감자") 처리
- 학습 데이터 (2026-07-08): **베이스라인=식약처 COOKRCP01(✅검증완료, 자유텍스트) + distant supervision**(재료 사전 gazetteer 자동라벨 + 정규식 분량·단위). **강화=농교원 재료정보(15058981, data.mafra.go.kr 등록)의 재료명+분량 structured → gold label.** 농교원 없이도 학습 성립, 있으면 품질↑
- **모델: CRF(Conditional Random Field) 기반 NER — CPU 학습+서빙** (KoBERT 대신; 3GB VRAM 제약)
- 재학습 주기: 새 레시피 크롤링 시 배치 재학습

**② 시세 예측 모델 [P0]** ✅ NEW — 실시간 데이터 최강 moat
- 목적: 실시간 도매 경락가 + 반입량 + 기상 → **내일/이번주 소매가 예측** ("배추 지금 사지 말고 3일 기다려라")
- ChatGPT-moat: **압도적.** 실시간 시계열 + 대량 집계 기반 예측은 상용 LLM 원천 불가
- 제품가치: "지금 사? 기다려?" = 앱 예산절약 핵심. 가장 잘 시연되는 AI
- 학습 데이터: 전국 32시장 실시간 경락가(15141808) + 반입량 + 기상청 초단기실황 시계열
- **모델: 시계열 예측(LightGBM/Prophet/시계열 회귀) — CPU 가능**
- 재학습 주기: 실시간 데이터 계속 유입 → 반복 재학습(K8s test #3) 자연 충족

**③ 신선도 예측 모델 [P1]**
- 목적: 식재료 종류 + 보관법(냉장/냉동/실온) + 온도 → 남은 신선도 기간 예측
- ChatGPT-moat: 실시간 조건 기반 예측은 사전학습 모델이 할 수 없음
- 학습 데이터: 식약처 소비기한 참고값 1,450품목 (PDF → 수동 파싱하여 테이블화, 1-2일 작업)
- **모델: XGBoost/LightGBM (tabular) — CPU 전용**
- 재학습 주기: 유저 피드백 데이터 축적 시

**④ 레시피 추천 랭킹 모델 [P1]**
- 목적: 냉장고 재고 + 유통기한 긴급도 + 예산 + 유저 이력 → 레시피 스코어링/랭킹
- ChatGPT-moat: 실시간 개인 냉장고 상태 + 유저 선호 학습 기반 랭킹은 범용 LLM이 못함
- 학습 데이터: 유저 상호작용 이력(클릭/조리/평가) — 콜드스타트는 규칙 기반 prior
- **모델: LightGBM (learning-to-rank) — CPU**
- 재학습 주기: 유저 활동 데이터 축적 시. **9주 초반엔 로그 없음 → 규칙기반으로 시작**

### 식재료 이미지 분류 → P1으로 강등 (데이터+하드웨어 트리플 악재)
- 이유: ① 3GB VRAM으로 CNN fine-tune 불가 ② AIHub 원재료 데이터셋 10종뿐(완성요리만 300만장) ③ Vision API 호출비용
- P0 냉장고 입력: **쇼핑 스크린샷 OCR(#2 범용 OCR) + 수동 입력**으로 대체
- P1에서 재검토: 범용 Vision API + 한국 식재료 정규화 후처리(하이브리드)

### 범용 AI (상용 모델 사용)

| 서비스 | 사용 모델 | 이유 |
|---|---|---|
| #2 외부 레시피 OCR | Naver Clova OCR / 범용 Vision | 범용 OCR로 충분 (커스텀 아님) |
| #10 외국 식재료 로컬라이징 (P1) | 범용 LLM | 번역+대체재 추천은 범용 LLM 강점 |

---

## §4. Kafka 데이터 파이프라인

### Kafka 정당화 (억지가 아닌 이유)
- **이기종 프로듀서**: 전국시장 실시간경매(장중 스트림), KAMIS 폴러(일별), 참가격 폴러(격주), 식품회수(이벤트), KOGL 크롤러(벌크), 유저 OCR(실시간) — 각각 다른 주기/형식
- **다중 컨슈머**: 같은 raw 데이터를 재료 NER, 가격 정규화, 검색 인덱싱, 유통기한 매칭이 각각 소비
- **리플레이**: 추출 로직 변경 시 전체 재가공 가능

### 토픽 설계 (초안, MSA 확정 후 구체화)

```
[프로듀서]                         [토픽]                        [컨슈머]
전국32시장 실시간경매 ⭐⭐⭐  →  raw.prices.market.stream →  시세 스트림 처리 → prices DB + 시세추천(멀티마켓)
식품회수 폴러(이벤트) ⚡       →  events.food.recall       →  냉장고 매칭기 → 알림 fan-out (스파이크)
레시피 API 폴러(식약처/농교원)  →  raw.recipes.api          →  정형 로더 → recipes DB (NER 불필요, gold label)
KOGL 벌크 크롤러(향토음식)     →  raw.recipes.crawled.bulk →  재료 NER 추출기 → recipes DB
KAMIS 폴러(일별)             →  raw.prices.kamis         →  가격 정규화기 → prices DB
참가격 폴러(격주)            →  raw.prices.chamgagyeok   →  가격 정규화기 → prices DB
기상청 초단기실황(시간)       →  raw.weather.now          →  시세예측 피처 스토어
유저 OCR 이벤트             →  events.user.ocr          →  재료 NER 추출기 → pantry DB
유저 활동 이벤트            →  events.user.activity     →  아카이빙 / 랭킹 모델 학습데이터
```
- 재료 NER 추출기 = `crawled.bulk` + `user.ocr` **2개 컨슈머** (레시피 API는 정형이라 NER 불필요)
- 갱신주기: **실시간(전국시장 경락가)** / 시간별(기상) / 일별(KAMIS) / 이벤트(회수) / 격주(참가격) / 분기(향토음식) = **이기종 스케줄 = K8s test #1 + 진짜 스트림**

---

## §5. MSA 서비스 분해

### 상시 운영 서비스 (Runtime) — 7개

| 마이크로서비스 | 담당 기능 (#) | DB 스키마 | 핵심 역할 |
|---|---|---|---|
| **Gateway** | — | — | API 라우팅, 인증 토큰 검증 |
| **User Service** | 회원가입/로그인/프로필 | `users` | auth, 유저 선호/예산 설정 |
| **Pantry Service** | #6, #3 | `pantry` | 냉장고 재고 CRUD + 유통기한 추적 + 알림 트리거 |
| **Recipe Service** | #4, #7(읽기), #2 | `recipes` + Elasticsearch | 레시피 검색/추천 + 외부 레시피 구조화 |
| **Price Service** | #9, #13 | `prices` + Redis 캐시 | 가격 조회/비교 + 시세 기반 추천 |
| **Meal Plan Service** | #1, #8 | `meal_plans` | 예산 밀플래닝 + 장바구니 생성 |
| **ML Serving Service** | AI 추론 전체 | — (모델 파일만) | 재료 NER + 시세 예측(P0) + 신선도 + 레시피 랭킹(P1). **CPU pod.** 통합 서비스, 모델별 endpoint 분리 |

### 배치/파이프라인 (Non-runtime) — 5개

| 컴포넌트 | 담당 | 스케줄 | K8s 리소스 |
|---|---|---|---|
| **Recipe Crawler** | #7 크롤링 → Kafka produce | 주간/일간 | CronJob |
| **Price Poller** | KAMIS(일별) + 참가격(격주) + 국가데이터처(일별) → Kafka | 소스별 상이 | CronJob |
| **NER Consumer** | Kafka consume → 재료 추출 → Recipe DB | 이벤트 드리븐 | Deployment (상시) |
| **Price Normalizer** | Kafka consume → 가격 정규화 → Price DB | 이벤트 드리븐 | Deployment (상시) |
| **ML Training Job** | 3종 모델 재학습 (CRF NER, XGBoost 신선도, LightGBM 랭킹) | 주간/월간 | Job (**CPU**) |

### 통신 패턴

| 패턴 | 구간 | 이유 |
|---|---|---|
| **REST (동기)** | Meal Plan → Recipe/Price/Pantry 서비스 호출 | 유저가 결과 기다림 |
| **REST (동기)** | 모든 서비스 → ML Serving 추론 요청 | 추론 결과 즉시 필요 |
| **Kafka (비동기)** | Crawler/Poller → Consumer들 | 데이터 수집 디커플링 |
| **Kafka (비동기)** | Pantry → 유통기한 임박 이벤트 → Notification | 알림은 비동기 |
| **Kafka (비동기)** | 유저 OCR 업로드 → 이미지 분류 → Pantry 업데이트 | 처리 시간 불확실 |

### DB 전략 (하이브리드)

```
PostgreSQL (공유 1대, 스키마 분리)
  ├── schema: users        ← User Service 전용
  ├── schema: pantry       ← Pantry Service 전용
  ├── schema: recipes      ← Recipe Service 전용
  ├── schema: prices       ← Price Service 전용
  └── schema: meal_plans   ← Meal Plan Service 전용

Elasticsearch (1대)
  └── index: recipes       ← 레시피 전문검색

Redis (1대)
  ├── 가격 캐시            ← Price Service
  └── 세션/토큰            ← Gateway/User Service

Kafka (클러스터)
  └── §4 토픽 구조 참조
```

**규칙:** 서비스 간 데이터 접근은 반드시 REST API를 통해서만. 다른 서비스의 스키마 직접 쿼리 금지.

---

## §6. K8s 4-test 충족 여부

| 테스트 | 판정 | 근거 |
|---|---|---|
| **1. 이기종 워크로드** | ✅ 통과 | CPU Deployment 7개 + CronJob 2개 + 이벤트 드리븐 Consumer 2개 + CPU Job(Training) + StatefulSet(Kafka/ES/Redis). GPU 제거로 워크로드 *형태* 다양성은 유지되나 GPU-vs-CPU 축은 상실 |
| **2. Train/Serve 공존** | ⚠️ 통과(약화) | ML Training Job(CPU, 주간 배치, high-mem 단발) ↔ ML Serving(CPU, 요청 기반 HPA, 저지연 상시). **GPU 없이도 batch Job vs HPA Deployment로 스케일링 정책은 다름** — 단 GPU node pool 분리라는 강력한 서사는 상실 |
| **3. 반복 재학습** | ✅ 통과 | 새 레시피 → NER 재학습, 유저 피드백 → 신선도 모델, 새 이미지 → 분류기. 데이터 drift가 진짜임 (인위적 아님) |
| **4. 스파이크 트래픽** | ✅ 보강됨 | ⚡ **식품 회수 발생 → 다수 유저 냉장고 매칭 → 알림 fan-out 스파이크** (자연 이벤트 스파이크 확보). + 식사 시간대 밀플래닝 집중 + 합성 부하(k6) 시연 보강 |

---

## §6.5 인증 (Auth)

**방식: Kakao 소셜 로그인 + 자체 이메일/비번 로그인 (둘 다)**

- **핵심 원칙:** 카카오든 자체든 **로그인 성공 지점에서 자체 JWT로 통일** → Gateway + 하류 6개 서비스는 로그인 방식 무관, JWT만 검증
- **유저 모델:** `users` 테이블 + `auth_providers` 분리 (provider='kakao'|'local', provider_user_id, password_hash[local만])
- **비번 저장:** bcrypt/argon2 해싱 (평문 금지)
- **토큰:** access(단기 ~15분) + refresh(장기, Redis 저장)
- **계정 연결(account linking):** MVP는 "이메일당 한 방식"으로 단순화, 멀티 연결은 P1
- **담당:** User Service(로그인/JWT 발급) + Gateway(JWT 검증)
- 카카오 OAuth2 authorization code flow 직접 구현 = 학습가치 (teach-manually)

## §6.6 CI/CD & 관측성

### CI/CD (GitOps)
| 단계 | 도구 | 역할 |
|---|---|---|
| CI (빌드/테스트) | **GitHub Actions** | push → 테스트 → Docker 빌드 → 레지스트리 push |
| 이미지 레지스트리 | **AWS ECR** | 컨테이너 이미지 저장 |
| CD (배포) | **ArgoCD (GitOps)** | Git manifest 변경 → K8s 자동 배포. 로컬→AWS 마이그레이션도 manifest 타깃만 변경 |
> [[project_booking_app]]에서 ArgoCD 경험(CRD 버그 포함). "Git=배포상태"가 kubeadm에 이상적.

### 관측성 — 풀스택 (LGTM)
| 대상 | 도구 | 도입 시점 |
|---|---|---|
| 메트릭 | **Prometheus + Grafana** | 로컬부터 |
| 로그 | **Loki** | AWS 이전 후 (로컬 32GB 부담) |
| 트레이스 | **Tempo** | 여유 시 |
| 대시보드 | **Grafana 통합** | — |
- **핵심 관측 대상 = 실시간 파이프라인:** Kafka consumer lag / 시세 수집 지연 / 예측모델 추론시간 → Grafana 데모 포인트
- ⚠️ 로컬 32GB에선 풀스택 무거움 → 로컬=Prometheus+Grafana만, AWS 이전 후 Loki/Tempo 추가

**폴백 옵션 (기록용):** VictoriaMetrics + Loki + Grafana + **Alloy** 조합 — [[project_monitoring_stack]]에서 운영 경험 있음. 리소스가 빡빡하면 VM(메모리 효율 Prometheus 대체)+Alloy(경량 수집기)로 전환 가능.

## §6.7 기술 스택 (확정)

**단일 언어(Python) 백엔드: FastAPI API + ML + 데이터 파이프라인 — 폴리글랏 세금 없음**

| 레이어 | 스택 |
|---|---|
| **Frontend** | React + Vite + TypeScript, PWA(반응형·카메라), TanStack Query + Zustand, Tailwind |
| **Backend API (Python)** | **FastAPI** — gateway, user/pantry/recipe/price/meal-plan. 인증=**PyJWT + FastAPI 의존성**(Kakao OAuth2 + 자체 JWT), **SQLAlchemy+Alembic**(PG), **confluent-kafka**, **Pydantic**(스키마) |
| **ML 서빙 (Python)** | **FastAPI** 통합 pod, 모델별 endpoint. **API와 동일 언어 → 모델·전처리 코드 공유(경계 없음)** |
| **데이터 파이프라인 (Python)** | 크롤러/폴러/컨슈머 (confluent-kafka), 재료 NER·가격정규화 |
| **저장소** | **PostgreSQL**(OLTP, CloudNativePG) + **ClickHouse**(OLAP 가격시계열·시세예측 피처, Altinity operator) + **Elasticsearch**(레시피검색) + **Redis**(가격캐시·세션) |
| **메시징** | **Kafka**(Strimzi operator) + **KEDA**(consumer lag 오토스케일) |
| **ML 학습** | CRF(sklearn-crfsuite, NER)·XGBoost(신선도)·LightGBM(랭킹·**시세예측 회귀**) — Python, **전부 CPU**. **Argo Workflows**(재학습) + **MLflow**(실험관리) |
| **Infra** | **kubeadm** on AWS · **Terraform** · **GitHub Actions + ECR + ArgoCD**(GitOps) · **Prometheus+Grafana+Loki+Tempo** · Docker |
| **External** | data.go.kr(도매시장 B552845·KAMIS·식품회수·기상청) · 식약처 COOKRCP01 · YouTube Data API · Naver Clova OCR · Kakao 로그인 |

→ **인프라 상세(컨테이너화·클러스터·클라우드)는 §6.8 (Docker / Kubernetes / AWS)로 분리 기록**

**단일 언어의 이점 (FastAPI 선택):**
- API·ML·파이프라인 전부 Python → 모델·재료사전·스키마·ClickHouse 쿼리 **코드 공유**, Java↔Python 경계·직렬화 홉 없음
- 공유 데이터: Kafka 토픽 + ClickHouse를 전 컴포넌트가 **동일 언어 클라이언트**로 접근
- 트레이드오프(포기한 것): Java-Spring 취업신호 · Spring Security 성숙도 → 팀 판단으로 FastAPI 선택

## §6.8 인프라 스택 (Docker / Kubernetes / AWS)

> 범례: **✅**=확정/필수 · **(표준)**=관행상 채택 제안(이견 없으면 확정) · **🟡**=보류/미정 · **⬜**=선택
> **원칙: Docker·Kubernetes 계층 = 클라우드 무관 (로컬 kubeadm 개발 1-6주, AWS 안 씀). AWS = 별도 계층 (마이그레이션 7-9주).**

### 🐳 Docker (컨테이너화 — 클라우드 무관)
- **전 서비스 멀티스테이지 빌드** ✅
  - 프론트: node 빌드 → **nginx:alpine 정적 서빙** (SPA 폴백 `try_files → index.html`, 정적자산 장기캐시). non-root=`nginxinc/nginx-unprivileged`
  - Python(API/ML/파이프라인): builder(의존성) → slim 런타임
- nginx = **프론트 정적 서빙 전용** (라우팅 아님)
- 이미지 스캔: **Trivy** (CI) ✅
- 레지스트리(개발): **Harbor** ✅ (UI + 취약점스캔 + 이미지 서명; 프로덕션은 AWS ECR)

### ☸️ Kubernetes (클러스터/플랫폼 — 클라우드 무관, 로컬 kubeadm)
- 배포: **kubeadm** full cluster (관리형 EKS 아님) ✅
- **CNI + 서비스 메쉬: 🟡 보류** — Cilium(eBPF·사이드카리스, 유저 경험有 → 유력) vs Calico+Linkerd
- 엣지: **K8s Gateway API 채택** ✅ / 구현체 **🟡 미정** (Cilium Gateway / Envoy Gateway / Traefik — CNI에 연동)
- 앱 게이트웨이: **FastAPI Gateway 서비스** ✅ (엣지 Gateway API → `/api` → FastAPI Gateway가 라우팅 + JWT 검증)
- 오토스케일(파드): **HPA + KEDA**(Kafka lag) + **metrics-server + Prometheus Adapter** ✅
- operator: **Strimzi**(Kafka)·**CloudNativePG**(PG)·**Altinity**(ClickHouse) ✅
- 인증서: **cert-manager** ✅ / 파드보안: **PSA**(restricted) ✅ + **NetworkPolicy**(CNI 제공, CNI 확정 후)
- GitOps: **ArgoCD** ✅ / ML 파이프라인: **Argo Workflows** ✅
- 관측: **kube-prometheus-stack + Loki + Tempo + OpenTelemetry** ✅ (Cilium이면 +Hubble)
- 클러스터 DNS: **CoreDNS**(내장) ✅
- 스토리지(개발): **local-path-provisioner** ✅ / LB(개발): **MetalLB** ✅ → AWS 단계에 EBS CSI·NLB(CCM)로 교체
- 시크릿(개발): **Sealed Secrets** ✅ (암호화 Git 커밋; AWS는 ESO+Secrets Manager)
- 정책: **Kyverno** ⬜ / 백업: **Velero** ⬜ (백엔드 S3=AWS)

### ☁️ AWS (클라우드 — 마이그레이션 7-9주, 로컬 kubeadm을 AWS로 이전. 개발 중엔 미사용)
- 컴퓨트: **EC2** (master+worker, worker=Spot) ✅
- CCM: **AWS Cloud Controller Manager** ✅ (K8s LoadBalancer → NLB, 노드 lifecycle)
- 노드 오토스케일: **Karpenter** ✅ (EC2 직접 프로비저닝)
- 네트워크: **NLB**(Gateway API 앞단) + VPC ✅
- 블록스토리지: **EBS**(gp3) + **EBS CSI Driver** ✅ (개발 local-path 대체)
- 오브젝트: **S3** (ML아티팩트/MLflow · 데이터레이크) ✅
- 레지스트리: **ECR** ✅ (개발 Harbor 대체)
- 시크릿: **External Secrets Operator + AWS Secrets Manager** ✅ (개발=Sealed Secrets)
- DNS: **Route53 + ExternalDNS** ✅
- 파드 AWS 권한: **IRSA** ✅ (OIDC 자가호스팅, 파드별 최소권한; kubeadm은 OIDC 프로바이더 직접 셋업 필요=고급). *Karpenter·ESO·S3 접근*
- IaC: **Terraform** ✅ / CI/CD: **GitHub Actions** ✅ (→ ECR → ArgoCD sync)

**마이그레이션 스왑 (로컬→AWS, 7주차 — 이 스왑 자체가 캡스톤 마이그레이션 서사):**
Harbor→ECR · local-path→EBS CSI · MetalLB→NLB(CCM) · (노드오토스케일 없음)→Karpenter · Sealed Secrets→ESO+Secrets Manager · +ExternalDNS/Route53

**✅ 스택 확정 (거의 완료):** FastAPI Gateway · IRSA · GitHub Actions+ArgoCD+Argo Workflows+MLflow · operator(Strimzi/CNPG/Altinity) · Sealed Secrets·local-path·MetalLB(개발) · KEDA·metrics-server·cert-manager·Trivy·PSA·OpenTelemetry · ECR·S3·Route53·ExternalDNS·Terraform · 프론트/백엔드/ML 라이브러리(§6.7)
**남은 결정 (🟡, 보류):** ① CNI+메쉬(Cilium 유력) ② Gateway API 구현체(①연동) · Kyverno·Velero(선택)
> ⚠️ 미확정 항목은 **사용자 승인 전까지 기록 금지** — 표준값 자동완성 금지.

## §7. 인프라 & 비용 (학생 예산)

### 학습 환경
| 환경 | 용도 | 비용 |
|---|---|---|
| 로컬 (16코어 CPU / 32GB) | CRF NER, XGBoost, LightGBM 학습 + 전체 서빙 | 무료 |
| Google Colab 무료 tier (T4 15GB) | transformer 실험 필요 시 (선택) | 무료 |
| AWS | 프로덕션 배포 (CPU만) | 아래 |

### K8s 배포 방식: **kubeadm full K8s on AWS (관리형 EKS 아님, AWS full migration)**
- 이유: 캡스톤은 인프라 깊이로 평가 → control plane(etcd/apiserver/controller/scheduler/CNI/인증서)을 직접 다루는 게 학습 목표. teach-manually 원칙 부합
- EKS 대비: control plane 비용 $0이지만 직접 운영(업그레이드/인증서/etcd 백업) 필요 = 학습 포인트
- 배포 위치: **AWS 단독** (바레메탈 [[project-k8s-lab]]은 프로덕션 토폴로지에서 제외; 로컬 개발용으로만 선택적 활용 가능)

### kubeadm-on-AWS 필수 통합 컴포넌트 (EKS면 자동, kubeadm이면 수동 = 인프라 점수)
| 컴포넌트 | 역할 | 미설치 시 |
|---|---|---|
| **AWS Cloud Controller Manager** | LoadBalancer Service → ELB, 노드 lifecycle | NodePort/수동 노출만 가능 |
| **EBS CSI Driver** | StatefulSet PV를 EBS로 | Kafka/ES/PG/Redis 영속화 불가 |
| **Cluster Autoscaler (EC2 ASG) 또는 Karpenter** | 워커 노드 오토스케일 | K8s 4-test #4(스파이크) 시연 불가 |
| **CNI (Calico/VPC CNI)** | Pod 네트워킹 | — |
| **IAM 인스턴스 프로파일** | CCM/CSI가 AWS API 호출 | 위 컴포넌트 동작 불가 |

- EBS는 AZ 종속 → StatefulSet Pod 스케줄링이 볼륨 AZ에 묶임 (설계 고려사항)

### AWS 월 비용 추정 (GPU 없음, kubeadm 셀프 control plane)
```
control plane EC2 (t3.medium): $30/월   (kubeadm master, 단일; EKS $72 대체)
t3.medium × 2-3 (워커):        $60-90/월 (Spot 시 ~$25-35/월)
PostgreSQL:                    $15/월   (RDS t3.micro) 또는 EC2 셀프호스트 $0
Redis:                         $13/월   (ElastiCache) 또는 셀프호스트 $0
────────────────────────────────────
최소(Spot+셀프호스트):         ~$85/월
최대(On-Demand+매니지드):      ~$220/월
```
- GPU 인스턴스 완전 제거 + kubeadm 셀프 control plane이 예산 방어의 핵심
- **비용 통제 전술 (AWS 상시 운영 시 필수):** ① 워커는 Spot ② 개발 외 시간엔 EC2 stop(종료 아님 → 컴퓨트 $0, EBS만 ~$2/월) ③ stateful(PG/Redis/Kafka/ES)은 매니지드 대신 클러스터 내 셀프호스트 ④ 단일 control plane(HA 3-master 아님)
- 스파이크(#4) 시연 = **Cluster Autoscaler on EC2 ASG**로 워커 노드 확장 (하이브리드 burst 대신 순수 AWS 오토스케일)

## §8. 결정 로그

| 일자 | 결정 | 근거 |
|---|---|---|
| 2026-07-08 | P0 = 냉장고 관리(#3,#6,#4) + 예산/가격(#1,#8,#9,#13) + 레시피 인프라(#2,#7) | 리서치에서 "full loop을 닫은 앱 없음"이 가장 강한 시그널 |
| 2026-07-08 | P1 = 소셜(#11), 병렬도우미(#5), 로컬라이징(#10), 아카이빙(#12) | 구현 난이도 높고 핵심 루프 없이도 MVP 성립 |
| 2026-07-08 | 가격 데이터 = KAMIS + 참가격 + 국가데이터처 3종 공공 API | 쿠팡/SSG/롯데 공식 API 없음 or 셀러전용. 크롤링=불법 |
| 2026-07-08 | 커스텀 AI 3종 = 재료 NER + 식재료 이미지 분류 + 신선도 예측 | ChatGPT-moat: 한식 비정형 계량, 한식 식재료 분류, 실시간 조건 예측 |
| 2026-07-08 | Kafka = 필수 (인프라 요구사항 + 이기종 프로듀서/다중 컨슈머로 자연 정당화) | 5종+ 데이터 소스 × 다수 컨슈머 × 리플레이 필요 |
| 2026-07-08 | 레시피 소스 = 농식품올바로 + 식약처 (합법 공공데이터) | 블로그/유튜브 크롤링 = 불법 |
| 2026-07-08 | OCR 대상 = P0: 온라인 쇼핑몰 주문 확인 스크린샷 / P1: 종이 영수증 | 온라인 주문 확인은 구조화된 레이아웃 → 범용 OCR 충분. 영수증은 축약어+물리적 손상으로 난이도 높아 후순위 |
| 2026-07-08 | ML Serving = 통합 단일 서비스로 시작, 모델별 분리 가능하게 설계 | 모델별 모듈/endpoint 분리 + 공유 상태 없음 → 나중에 Deployment 추가만으로 분리 가능 |
| 2026-07-08 | MSA = 상시 서비스 7개 + 배치 5개 = 12 배포 단위 | Gateway, User, Pantry, Recipe, Price, MealPlan, ML Serving + Crawler, Poller, NER Consumer, Price Normalizer, ML Training |
| 2026-07-08 | 통신 = 유저 대면 REST + 백엔드 파이프라인 Kafka. gRPC 미사용 | 5명 팀 운영 부담 고려. ML 호출도 REST 충분 |
| 2026-07-08 | DB = 하이브리드: PostgreSQL 1대(스키마 분리 5개) + Elasticsearch(레시피 검색) + Redis(가격 캐시+세션) | 서비스별 완전 분리는 5명 팀에 운영 과부하. 스키마 격리 + REST-only 접근 규율로 보장 |
| 2026-07-08 | **하드웨어 제약: GTX 1060 3GB + 학생 예산 → 모든 커스텀 AI를 CPU로** | GPU fine-tune 불가 + GPU 인스턴스 월 $380 배제 |
| 2026-07-08 | 커스텀 AI 3종 수정: **CRF NER + XGBoost 신선도 + LightGBM 레시피 랭킹** (전부 CPU) | KoBERT→CRF(VRAM 제약), 이미지 분류→P1 강등, 빈자리에 레시피 랭킹 추가 |
| 2026-07-08 | 식재료 이미지 분류 → P1 강등 | 3GB VRAM 불가 + AIHub 원재료 10종뿐 + Vision API 비용 (트리플 악재) |
| 2026-07-08 | AI 학습데이터 검증 완료 | NER=농교원 structured gold label✅ / 신선도=식약처 1,450품목 PDF 수동파싱 / 이미지=원재료 데이터 부족 |
| 2026-07-08 | AWS는 CPU-only, Spot+셀프호스트로 월 ~$85-100 목표 | 학생 예산. K8s-lab 하이브리드도 옵션 |
| 2026-07-08 | **K8s = kubeadm full cluster (관리형 EKS 아님)** | 캡스톤 인프라 깊이 평가 + teach-manually. control plane 직접 운영이 학습 목표. EKS $72/월 절감 |
| 2026-07-08 | **배포 = AWS 단독 (full migration), 바레메탈은 프로덕션 제외** | 사용자 결정. 바레메탈은 로컬 개발용으로만 선택적 |
| 2026-07-08 | kubeadm-on-AWS 통합: CCM + EBS CSI + Cluster Autoscaler + CNI + IAM 직접 설치 | EKS 자동 제공분을 수동 = 인프라 점수. 스파이크 시연은 EC2 ASG 오토스케일 의존 |
| 2026-07-08 | **개발 방식 = B: 로컬 kubeadm 개발(1-6주, $0) → AWS 마이그레이션(7-9주)** | 비용 총 $50-80로 억제. "로컬→클라우드 마이그레이션(Terraform IaC)" 자체가 캡스톤 클라이맥스 |
| 2026-07-08 | 레시피 데이터 이원화: API(정형, 폴링) + KOGL/CC 크롤(비정형) | 크롤링 요구사항 충족 + 크롤 비정형 콘텐츠가 재료 NER을 load-bearing하게 만듦(상호 정당화) |
| 2026-07-08 | 클라이언트 = C: React 반응형 웹/PWA (네이티브 모바일 아님) | 웹 제약 충족 + 모바일 브라우저 카메라(`<input capture>`)로 OCR UX 성립 + 인프라에 집중 |
| 2026-07-08 | **크롤 명분 검증: 향토음식/제철 크롤은 정적(스트림 아님) → 배치.** 연속 크롤 근거는 위키 EventStreams | 검증 결과 향토 레시피는 일회성 벌크. 정직하게 3계층 분리(배치/스트림/API폴) |
| 2026-07-08 | **소스 3종 추가: 위키 EventStreams(연속 크롤) + NEIS 급식(일별 대량) + 식품회수(이벤트)** | 주기적 크롤 명분 + NER 학습데이터 대폭 강화 + 냉장고 안전알림 신기능 + 스파이크 소스(#4 보강) |
| 2026-07-08 | **★★ 전국 32개 공영도매시장 실시간 경매정보(data.go.kr/15141808, aT)를 hero 스트림으로 채택** | 단일 API로 전국 32개 시장 커버 → 단일소스 리스크 해소 + 멀티마켓 가격비교 신기능 + 스트리밍 볼륨↑(Kafka 파티셔닝 명분). 가락 단독(15004517)은 부분집합, 선택적 고해상도로만 유지. 위키피디아는 폴백 |
| 2026-07-08 | "크롤=학습" 등식 폐기: 학습=정형API / 실시간=가락시세 / 크롤=런타임콘텐츠 축 분리 | Common Crawl은 지도학습 NER에 무의미(라벨 없음). 만개레시피 유혹 근원 제거. 크롤은 추론 입력 공급 역할 |
| 2026-07-08 | 한국 TDM 면책 미통과 확인(2026.7): AI 학습 목적도 사설 크롤 불법 | 도종환/황보승희 개정안 계류. 2026.2 공정이용 안내서 나왔으나 부정적 견해 지배 |
| 2026-07-08 | **커스텀 AI 4종화 + 우선순위: P0=재료NER+시세예측 / P1=신선도+레시피랭킹** | 시세예측이 실시간 데이터 최강 moat + Day1 학습데이터 존재. 레시피랭킹은 유저로그 축적 필요→P1(콜드스타트 규칙기반) |
| 2026-07-08 | 인증 = Kakao 소셜 + 자체 이메일/비번 둘 다. 로그인 성공점에서 자체 JWT 통일 | 완결성 + 학습가치. Gateway/하류서비스는 JWT만 검증(로그인 방식 무관). 계정연결은 P1 |
| 2026-07-08 | CI/CD = GitHub Actions + ECR + ArgoCD(GitOps). 관측성 = Prometheus+Grafana+Loki+Tempo 풀스택 | 폴백으로 VictoriaMetrics+Alloy 기록. 로컬=Prom+Grafana만, AWS후 Loki/Tempo 추가 |
| 2026-07-08 | **데이터소스 실검증 실시(§2.5): 식약처·NEIS 라이브 확인 / 8개 실존확인 / 메뉴젠(15081047) 404 검출·드롭** | fork 보고 맹신 리스크 실제 검출. 리스크는 키 발급 후 확인 |
| 2026-07-08 | **적합성 감사(§2.6): 가락8초·위키·NEIS 드롭, 온라인가격 P1 강등** | "기능이 소비 vs 명분용 억지" 기준. NEIS=요리명뿐 NER부적합+볼륨명분 중복, 가락8초=전국피드 부분집합, 위키=순수 크롤명분 폴백. 남은 소스 전부 실기능 소비 |
| 2026-07-08 | **참가격(3043385) API DEAD 실검증 → #9 재구성(B)** | `/openApiImpl/ProductPriceInfoService/` 6/6 변형 404(모든 op·양쪽 호스트·http/https). 참고문서도 죽은 경로. **한국에 매장명별 가격비교 합법소스 부재 확정.** #9 코어="KAMIS 지역평균 대비"(P0), 온라인 최저가(온라인가격 or 네이버쇼핑)는 P1 옵션. "매장명 콕집기"는 폐기 |
| 2026-07-08 | **KAMIS 라이브 실증 ✅ (#9 새 근간 확인)** | 참가격 죽은 직후 #9 근간이 된 KAMIS를 실호출 → error_code:000 + 2026-07-08 실데이터. 엔드포인트 생존 확정. 참가격과 달리 #9 재구성이 실데이터 위에 섬. 지역별 상세는 정식 키 발급 후 |
| 2026-07-08 | **쿠팡/네이버 크롤 금지 재확인** | 쿠팡=형사판례/네이버=ToS라 크롤 배제 (비상/개인용도 정보통신망법·DB권 무관) |
| 2026-07-08 | **네이버 쇼핑 검색 API 2026-07-31 종료(사용자 제보) → 가공식품 가격 전략 재구성** | 참가격+네이버 연쇄 죽음 = 가공식품 시장가격 합법소스 부재 확정. **전략 전환: 가격 지능=신선식품(KAMIS/전국시장) 올인**(변동↑·예측가치↑·합법). 가공식품은 유저 영수증 OCR(본인데이터)로 개인추적, 시장비교 포기. 카카오쇼핑 API는 확인 대기(사용자: 나중에) |
| 2026-07-08 | **★ 키 활성 확정 + HERO/#9근간 실검증 완료 (최대 리스크 종결)** | 정식 일반인증키로 `B552845/perDay/price`=200 정상+실가격(쌀 서울 중도매 59,400원, 전체필드), `katRealTime2/trades2`=200 정상. 초기 401은 전파대기/파라미터(saleDate 대신 `cond[]` DSL) 문제였음. **HERO 전국32시장 살아있음+인증됨(참가격과 정반대), #9 신선식품 근간 실데이터 확정.** 파라미터 문법=`cond[fld::EQ/LTE/GTE]=값`+returnType+selectable |
| 2026-07-08 | **레시피 소스 검증예정 목록화 + 영상추출 MVP=YouTube만 확정** | 레시피: 식약처(실키 언블락) → 농교원(mafra)/농식품올바로 후속. 영상 URL→재료추출: MVP는 **YouTube 설명란(Data API)→NER**만, 틱톡/릴스 후속. 합법 가드레일=유저 건별·개인용·비저장(대량크롤 저장 금지). #2 확장, NER 재사용 |
| 2026-07-09 | **기술 스택 확정(§6.7): FastAPI(Python) 단일언어 백엔드 + PostgreSQL+ClickHouse** (07-08 Spring 선택→재검토 후 변경) | 이유=프로젝트가 ML/데이터 중심(전부 Python)이라 단일언어가 Java↔Python 경계를 통째 제거, 9주 5인에 유리. 포기=Java 취업신호·Spring Security 성숙도. 저장소=PG(OLTP)+ClickHouse(가격시계열/시세예측). operator 4종(Strimzi/CNPG/Altinity/KEDA) 재사용 |
| 2026-07-08 | **Docker=전 서비스 멀티스테이지(프론트 nginx:alpine), 엣지=K8s Gateway API 채택** | 프론트 nginx는 정적 서빙만. K8s Gateway API(Ingress 대체 최신표준)=엣지 인프라 쇼케이스. **구현체(Envoy/Traefik)·앱 게이트웨이 방식은 미정** — 사용자 결정 대기 (이전에 임의 확정했다가 정정) |
| 2026-07-09 | **인프라 스택 Docker/K8s/AWS 3계층 분리 기록(§6.8). Karpenter·External Secrets+Secrets Manager 확정** | AskUserQuestion으로 노드오토스케일=Karpenter, 시크릿=ESO+Secrets Manager 확정. **CNI+메쉬 보류**(Cilium eBPF/사이드카리스 유력 — 유저 경험有 — vs Calico+Linkerd). Gateway API 구현체·앱게이트웨이·파드AWS권한(IRSA)은 미정. 표준항목(cert-manager/Trivy/PSA/OTel/ExternalDNS/S3)은 이견없으면 확정 |
| 2026-07-09 | **인프라 3계층 원칙 정정: Docker·K8s=클라우드 무관(로컬 개발, AWS 미사용) / AWS=별도 계층(마이그레이션)** | AWS결합 항목(ECR·EBS CSI·CCM·Karpenter·S3·Secrets Manager·Route53·ESO·ExternalDNS·IRSA)을 전부 AWS 계층으로 이동. K8s는 클라우드 무관 컴포넌트만(로컬은 local-path·NodePort 등, 7주차 마이그레이션 스왑). "로컬→AWS 스왑" 자체가 캡스톤 서사 |
| 2026-07-09 | 개발 레지스트리=**Harbor** 확정. **미확정 스택 전부 사용자 확인 후 기록 원칙 강화** | 사용자 지시: 명시 확정 외 미정 스택 다 물어보고 기록, 표준값 자동완성 금지(로컬레지스트리 임의기입 정정). 남은결정 ①~⑩ 순차 진행 |
| 2026-07-09 | **배치2 확정: 앱게이트웨이=FastAPI Gateway · 파드권한=IRSA · CI/CD/ML=(GitHub Actions/ArgoCD/Argo Workflows/MLflow) · operator=전용(Strimzi/CNPG/Altinity) · 개발시크릿=Sealed Secrets · 개발스토리지·LB=local-path+MetalLB** | CNI+메쉬·Gateway API 구현체 보류 유지. 표준 애드온(KEDA/metrics/cert-manager/Trivy/PSA/OTel/ExternalDNS/Terraform/ECR/S3/Route53)+라이브러리는 승인 목록 제시 후 기록 |
| 2026-07-09 | **배치3 승인: 표준 애드온 + AWS기본 + 프론트/백엔드/ML 라이브러리 확정. 시세예측=LightGBM 회귀** | KEDA/metrics/cert-manager/Trivy/PSA/OTel + ECR/S3/Route53+ExternalDNS/Terraform + Vite/TS/TanStack Query/Zustand/Tailwind + SQLAlchemy+Alembic/PyJWT/confluent-kafka/Pydantic/sklearn-crfsuite/XGBoost/LightGBM. **Kyverno·Velero 보류(선택).** 기술스택 거의 완료 — CNI+메쉬·Gateway API 구현체만 보류 |
