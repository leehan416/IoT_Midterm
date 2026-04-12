import asyncio
import logging

from app.repository import mqtt_repository

logger = logging.getLogger(__name__)


async def check_broker_status(host: str, port: int, timeout: float = 2.0) -> bool:
    """상대방의 포트가 열려있는지 확인하는 함수
        연결이 성공하면 true를 반환하고, 실패하면 false를 반환한다. 
        기본 timeout은 2초로 설정됨.
    """
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=timeout 
        )
        writer.close()
        await writer.wait_closed()
        return True
    except Exception:
        return False


async def mqtt_status_checker(interval: int = 10):
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
            brokers = await mqtt_repository.get_all_mqtt_datas()
            for broker in brokers:
                is_active = await check_broker_status(broker.host, broker.port)
                if broker.is_active != is_active:
                    logger.info(f"Broker {broker.id} ({broker.host}:{broker.port}) changed status: {broker.is_active} -> {is_active}")
                    broker.is_active = is_active
                    await mqtt_repository.save_mqtt_data(broker)
        except Exception as e:
            logger.error(f"Error during MQTT status check: {e}")
            
        await asyncio.sleep(interval)
