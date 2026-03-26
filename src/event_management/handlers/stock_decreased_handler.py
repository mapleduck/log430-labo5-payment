from typing import Dict, Any
from event_management.base_handler import EventHandler
from event_management.order_event_producer import OrderEventProducer
from commands.write_payment import create_payment
import config

class StockDecreasedHandler(EventHandler):
    
    def __init__(self):
        super().__init__()
    
    def get_event_type(self) -> str:
        return "StockDecreased"
    
    def handle(self, event_data: Dict[str, Any]) -> None:
        try:
            payment_id = create_payment(
                order_id=event_data['order_id'],
                user_id=event_data['user_id'],
                total_amount=event_data['total_amount']
            )
            event_data['payment_id'] = payment_id
            event_data['payment_link'] = f"http://api-gateway:8080/payments-api/payments/process/{payment_id}"
            event_data['event'] = "PaymentCreated"
            self.logger.debug(f"Paiement créé: payment_id={payment_id}")
        except Exception as e:
            event_data['event'] = "PaymentCreationFailed"
            event_data['error'] = str(e)
            self.logger.error(f"Erreur création paiement: {e}")
        finally:
            OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)