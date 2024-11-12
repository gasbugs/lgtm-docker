import asyncio
import time
import random
from flask import Flask, request
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
import logging
import requests
from functools import wraps

# 로깅 설정: INFO 레벨로 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# OpenTelemetry 설정
# 서비스 이름을 "complex-flask-demo"로 지정하여 리소스 생성
resource = Resource.create({"service.name": "complex-flask-demo"})
# 트레이서 프로바이더 설정
trace.set_tracer_provider(TracerProvider(resource=resource))
# OTLP 익스포터 설정 (로컬호스트의 4317 포트로 데이터 전송)
otlp_exporter = OTLPSpanExporter(endpoint="http://localhost:4317")
# 배치 스팬 프로세서 생성 및 트레이서 프로바이더에 추가
span_processor = BatchSpanProcessor(otlp_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

# Flask 애플리케이션 생성
app = Flask(__name__)
# Flask 애플리케이션 자동 인스트루멘테이션
FlaskInstrumentor().instrument_app(app)
# HTTP 요청 자동 인스트루멘테이션
RequestsInstrumentor().instrument()
# 로깅 자동 인스트루멘테이션
LoggingInstrumentor().instrument()

# 트레이서 생성
tracer = trace.get_tracer(__name__)

# 비동기 작업을 위한 데코레이터
def async_action(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(f(*args, **kwargs))
        finally:
            loop.close()
    return wrapped

# 비동기 작업 시뮬레이션 함수
async def async_operation(name):
    with tracer.start_as_current_span(f"async_{name}"):
        await asyncio.sleep(random.uniform(0.1, 0.5))
        logger.info(f"Async operation {name} completed")

# 외부 API 호출 시뮬레이션 함수
def external_api_call(url):
    with tracer.start_as_current_span("external_api_call"):
        response = requests.get(url)
        logger.info(f"External API call to {url} completed with status {response.status_code}")
        return response.json()

# 복잡한 작업을 수행하는 엔드포인트
@app.route('/complex-operation')
@async_action
async def complex_operation():
    with tracer.start_as_current_span("complex_operation"):
        logger.info("Starting complex operation")
        
        # 데이터베이스 쿼리 시뮬레이션
        with tracer.start_as_current_span("database_query"):
            await asyncio.sleep(random.uniform(0.1, 0.3))
            logger.info("Database query completed")
        
        # 데이터 처리 시뮬레이션
        with tracer.start_as_current_span("processing"):
            await asyncio.sleep(random.uniform(0.2, 0.4))
            logger.info("Data processing completed")
        
        # 여러 비동기 작업 동시 실행
        await asyncio.gather(
            async_operation("task1"),
            async_operation("task2"),
            async_operation("task3")
        )
        
        # 외부 API 호출
        external_data = external_api_call("https://jsonplaceholder.typicode.com/todos/1")
        
        # 최종 계산 시뮬레이션
        with tracer.start_as_current_span("final_computation"):
            await asyncio.sleep(random.uniform(0.1, 0.2))
            logger.info("Final computation completed")
        
        return {"message": "Complex operation completed", "external_data": external_data}

# 메인 실행 부분
if __name__ == '__main__':
    app.run(debug=True, port=5000, host="0.0.0.0")
