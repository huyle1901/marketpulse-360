"""Viewer-level insights HTTP routes; errors map through the lifecycle handler."""
from fastapi import FastAPI

from .analytics import Analytics
from .insights import InsightsService


def register_insights_routes(app: FastAPI, analytics: Analytics) -> None:
    service = InsightsService(analytics)

    @app.get('/api/v1/insights/activity')
    def activity(): return service.activity_heatmap()

    @app.get('/api/v1/insights/order-values')
    def order_values(): return service.order_values()

    @app.get('/api/v1/insights/loyalty')
    def loyalty(): return service.loyalty()

    @app.get('/api/v1/insights/channels')
    def channels(): return service.channels()

    @app.get('/api/v1/insights/products')
    def products(): return service.products()

    @app.get('/api/v1/insights/geography')
    def geography(): return service.geography()
