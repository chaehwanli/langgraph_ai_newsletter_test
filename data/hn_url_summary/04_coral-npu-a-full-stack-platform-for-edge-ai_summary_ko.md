원본 URL: https://research.google/blog/coral-npu-a-full-stack-platform-for-edge-ai/
원본 제목(영문): Coral NPU: A full-stack platform for Edge AI
번역 제목(한글): Coral NPU: 엣지 AI를 위한 풀스택 플랫폼
구글은 배터리 제약이 큰 엣지·웨어러블에서 항상 켜진 개인화 AI를 가능하게 하기 위해, 성능 격차·플랫폼 파편화·프라이버시 문제를 해결하는 풀스택 오픈소스 플랫폼 Coral NPU를 발표했습니다. Coral NPU는 RISC‑V 기반의 AI‑우선 NPU 아키텍처로 스칼라 코어, 벡터 유닛, 행렬 연산 유닛을 포함해 초저전력 온디바이스 추론에 최적화되어 있습니다. IREE·MLIR·TFLM 등 표준 툴체인과 통합되어 TensorFlow/JAX/PyTorch 모델을 일관되게 배포할 수 있으며, 인코더 기반 비전·오디오와 소형 트랜스포머(예: Gemma)를 가속해 웨어러블에서의 LLM 실행을 지향합니다. 주요 활용은 컨텍스트 인식, 음성·오디오 처리, 저전력 영상 인식 등이며, CHERI와 같은 하드웨어 보안을 통해 민감한 모델과 데이터를 격리하는 개인정보 보호를 강화합니다. 또한 시냅틱스와 협력해 Coral NPU의 첫 상용 구현(Torq NPU)을 선보이는 등 개방형 표준과 통합된 개발자 경험을 확산해 엣지 컴퓨팅의 성능 격차와 파편화를 해소하는 생태계를 구축하고자 합니다.
