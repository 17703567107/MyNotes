
import datetime

import pymysql
# 打开数据库连接
import requests

conn = pymysql.connect(host='oyo-iot-database.mysql.rds.aliyuncs.com', port=3306, database='buildpeople', user='oyo_admin',
                       password='Do#(!m62s0', charset='utf8')

# 使用cursor()方法获取操作游标
cursor = conn.cursor()
file = open('./hotelUpdateLog.txt', 'a')
file.write(str(datetime.datetime.now()) + '\n')
file.write('=' * 150 + '\n')
res = requests.post(url='http://ali.ahotels.tech/product-service/hotel/info/list',
                    json={"pageNum": 1, "pageSize": 100})
total_pages = res.json()['data']['totalPages']
for page in range(1, total_pages + 1):
    response = requests.post(url='http://ali.ahotels.tech/product-service/hotel/info/list',
                             json={"pageNum": page, "pageSize": 100})

    for result in response.json()['data']['result']:

        data = ('null' if not result.get('hotelId', '') else f"\"{result.get('hotelId', '')}\"",
                'null' if not result.get('oyoId', '') else f"\"{result.get('oyoId', '')}\"",
                'null' if not result.get('hotelName', '') else f"\"{result.get('hotelName', '')}\"",
                'null' if not result.get('enName', '') else f"\"{result.get('enName', '')}\"",
                'null' if not result.get('alternateName', '') else f"\"{result.get('alternateName', '')}\"",
                'null' if not result.get('phone', '') else f"\"{result.get('phone', '')}\"",
                'null' if not result.get('status', '') else f"\"{result.get('status', '')}\"",
                'null' if not result.get('type', '') else f"\"{result.get('type', '')}\"",
                'null' if not result.get('bizDate', '') else f"\"{result.get('bizDate', '')}\"",
                'null' if not result.get('uniqueCode', '') else f"\"{result.get('uniqueCode', '')}\"",
                'null' if not result.get('street', '') else f"\"{result.get('street', '')}\"",
                'null' if not result.get('cityId', '') else f"\"{result.get('cityId', '')}\"",
                'null' if not result.get('cityName', '') else f"\"{result.get('cityName', '')}\"",
                'null' if not result.get('hubId', '') else f"\"{result.get('hubId', '')}\"",
                'null' if not result.get('hubName', '') else f"\"{result.get('hubName', '')}\"",
                'null' if not result.get('zoneId', '') else f"\"{result.get('zoneId', '')}\"",
                'null' if not result.get('zoneName', '') else f"\"{result.get('zoneName', '')}\"",
                'null' if not result.get('signRoomNum', '') else f"\"{result.get('signRoomNum', '')}\"",
                'null' if not result.get('floor', '') else f"\"{result.get('floor', '')}\"",
                'null' if not result.get('brandType', '') else f"\"{result.get('brandType', '')}\"",
                'null' if not result.get('countryName', '') else f"\"{result.get('countryName', '')}\"",
                'null' if not result.get('provinceId', '') else f"\"{result.get('provinceId', '')}\"",
                'null' if not result.get('provinceName', '') else f"\"{result.get('provinceName', '')}\"",
                'null' if not result.get('clusterId', '') else f"\"{result.get('clusterId', '')}\"",
                'null' if not result.get('clusterName', '') else f"\"{result.get('clusterName', '')}\"",
                'null' if not result.get('isDeleted', '') else f"\"{result.get('isDeleted', '')}\"",
                'null' if not result.get('latitude', '') else f"\"{result.get('latitude', '')}\"",
                'null' if not result.get('longitude', '') else f"\"{result.get('longitude', '')}\"")
        sql = """
        INSERT INTO binding_hotel (
        hotelId,
        oyoId,
        hotelName,
        enName,
        alternateName,
        phone,
        STATUS,
        type,
        bizDate,
        uniqueCode,
        street,
        cityId,
        cityName,
        hubId,
        hubName,
        zoneId,
        zoneName,
        signRoomNum,
        floor,
        brandType,
        countryName,
        provinceId,
        provinceName,
        clusterId,
        clusterName,
        isDeleted,
        latitude,
        longitude
    )
    VALUES
        (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
    
        ) ON DUPLICATE KEY UPDATE hotelId = %s,oyoId = %s,hotelName = %s,enName = %s,alternateName = %s,phone = %s,STATUS = %s,type = %s,bizDate = %s,uniqueCode = %s,street = %s,cityId = %s,cityName = %s,hubId = %s,hubName = %s,zoneId = %s,zoneName = %s,signRoomNum = %s,floor = %s,brandType = %s,countryName = %s,provinceId = %s,provinceName = %s,clusterId = %s,clusterName = %s,isDeleted = %s,latitude = %s,longitude = %s
                """ % (data * 2)
        try:
            cursor.execute(sql)
            conn.commit()
        except Exception as e:
            file.write(f"error:{str(e)}" + '\n')
            file.write(f'data:{str(result)}' + '\n')
            conn.rollback()
            continue
file.write('=' * 150 + '\n')
file.close()
