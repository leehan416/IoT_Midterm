import asyncio
import logging

import app.services.mqtt_service as mqtt_service

logger = logging.getLogger(__name__)


async def mqtt_status_checker(interval: int = 1):
    """
    1. get_all_mqtt_datas : redis에서 모든 broker의 정보를 가져온다. 
    2. check_brokers_status : 만약에 이전 정보와 현재 정보가 다르다면, 로그를 남긴다. 
    3. save_mqtt_data : 변경된 정보를 redis에 저장한다. 

    우선 10초에 한번씩 체크를 하는 것으로 설정을 해두었음. 
    그런데 publisher쪽에서 broker에 문제가 생기는 것을 인지하고 server에 살아있는 broker를 요청했을때,
    이 함수가 실행되기 전이라면, 죽은 broker를 반환할 수도 있다. 
    
    그러므로 publisher측에서 살아있는 broker를 요청했을때, event driven 형식으로 status check하고
    그 결과를 바로 반환하는 것이 좋을 것 같다. 

    """
    logger.info(f"MQTT checker task started. Interval: {interval} seconds.")
    while True:
        try:
            await mqtt_service.sync_mqtt_broker_statuses()
        except Exception as e:
            logger.error(f"Error during MQTT status check: {e}")
            
        await asyncio.sleep(interval)
