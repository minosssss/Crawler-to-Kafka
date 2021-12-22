from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
# Consumer Setting
consumer = KafkaConsumer(
    'test',
    bootstrap_servers=['localhost:9092'],  # 클러스터 호스트와 포트정보
    auto_offset_reset='earliest',  # 가장 빠른 것부터(Counsumer에겐 가장 먼 것)
    enable_auto_commit=True,  # 백그라운드로 주기적 offset 커밋
    group_id='my-group',  # 속한 그룹을 식별
    # Producer에서 value_serializer를 사용했기에 적용
    value_deserializer=lambda x: loads(x.decode('utf-8')),
    consumer_timeout_ms=1000)  # 데이터가 없으면 10초 후 자동 종료

# Mongo 클라이언트 연결
client = MongoClient('mongodb://team1:team1@localhost', 27017)
db = client.test  # test DB 사용

# consumer list를 가져온다


def consumer_insert():
    db.team1.drop()  # 기존 데이터 제거
    for message in consumer:
        message = message.value
        db.team1.insert_one(message)  # test 내 team1 테이블에 저장
        print('{} added to'.format(message))


# 함수 실행하기
consumer_insert()
