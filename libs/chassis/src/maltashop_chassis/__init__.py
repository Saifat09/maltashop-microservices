"""MaltaShop microservice chassis.

Cross-cutting code that every service needs and that should behave the same
everywhere: structured JSON logs, correlation-ID propagation, the transactional
outbox, idempotent consumers and the RabbitMQ event bus. Business logic never
lives here.
"""

__version__ = "0.1.0"
