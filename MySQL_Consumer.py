from kafka import KafkaConsumer
import pandas as pd
import numpy as np
import pymysql
from json import loads
from pandas import json_normalize
from sqlalchemy import create_engine

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

# MySQL Setting
conn = pymysql.connect(host='localhost',
                       port=3306,
                       user='root',
                       password='root',
                       db='test')
# Connection 으로부터 Cursor 생성 (Host 내 DB직접 연결)
cursor = conn.cursor()

# Pandas에서 엔진을 이용하여 파일 직접 저장 및 읽어오는 라이브러리
engine = create_engine("mysql+pymysql://root:"+"root" +
                       "@localhost:3306/test?charset=utf8", encoding='utf-8')

# consumer list를 가져오기
print(consumer)

# MySQL 테이블 리셋,생성,추가 함수


def consumer_insert():
    # 기존 테이블을 삭제
    cursor.execute("DROP TABLE IF EXISTS test1")
    # 테이블 신규 생성
    create_table = '''
    CREATE TABLE test1 (
    `id` int AUTO_INCREMENT,
    `notice` VARCHAR(200) NOT NULL, 
    `url` text NOT NULL NOT NULL, 
    `company` VARCHAR(200) NOT NULL,
    `Position` text NULL, 
    `Employment Type` text NULL,
    `Experiences` text NULL, 
    `Company Size` text NULL,
    `Principal Services` text NULL, 
    `Period` text NULL, 
    `Work Location` text NULL, 
    `Salary` text NULL, 
    PRIMARY KEY(id))
    '''
    cursor.execute(create_table)
    # 쿼리지정
    query = "INSERT INTO test1 (`notice`, `url`, `company`, `Position`, `Employment Type`, `Experiences`, `Company Size`, `Principal Services`, `Period`, `Work Location`, `Salary` ) VALUES (%s, %s, %s, %s, %s, %s ,%s ,%s, %s, %s, %s)"
    for message in consumer:
        message = message.value

        n = message['notice'].strip('')
        u = message['url'].strip('')
        c = message['company'].strip('')
        info = message['information']
        p = info.get('Position')
        em = info.get('Employment Type')
        ex = info.get('Experiences')
        cs = info.get('Company Size')
        ps = info.get('Principal Services')
        p = info.get('Period')
        wl = info.get('Work Location')
        s = info.get('Salary')
        data = n, u, c, p, em, ex, cs, ps, p, wl, s
        print(data)
        cursor.execute(query, data)
    conn.commit()


# 함수 실행명렁
consumer_insert()
