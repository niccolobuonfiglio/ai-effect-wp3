"""Service health and manual recovery routes.

Exposed as a factory returning an ``APIRouter`` so the routes can be mounted by
``OrchestratorAPI`` while keeping their dependencies explicit.

Endpoints:
    GET    /orchestrator/health/services
    GET    /orchestrator/health/services/{service_name}
    POST   /orchestrator/health/services/{service_name}/recover
    DELETE /orchestrator/health/services/{service_name}
"""

import logging

from fastapi import APIRouter, Depends, HTTPException

from api.models import (
    DeregisterResponse,
    ErrorResponse,
    RecoveryResponse,
    RegisteredServiceResponse,
    ServiceHealthResponse,
    ServicesHealthResponse,
)
from services.circuit_breaker import HEALTHY
from services.service_registry import ServiceRegistry

logger = logging.getLogger(__name__)


def create_health_router(
    registry: ServiceRegistry,
    dependencies: list | None = None,
) -> APIRouter:
    """Build the health router bound to a registry.

    Args:
        registry: Service registry backing all health lookups.
        dependencies: FastAPI dependencies applied to every route - pass the
            orchestrator API-key verifier here.
    """
    if registry is None:
        raise ValueError("registry is required")

    router = APIRouter(
        prefix="/orchestrator/health",
        tags=["health"],
        dependencies=list(dependencies or []),
    )

    @router.get(
        "/services",
        response_model=ServicesHealthResponse,
        summary="Status of all registered services",
    )
    def get_services_health() -> ServicesHealthResponse:
        """Self-service dashboard: status of every service the orchestrator knows."""
        summary = registry.get_summary()
        return ServicesHealthResponse(
            overall_status=summary.overall_status,
            total=summary.total,
            healthy=summary.healthy,
            degraded=summary.degraded,
            unhealthy=summary.unhealthy,
            services=[
                ServiceHealthResponse(**health.to_dict()) for health in summary.services
            ],
        )

    @router.get(
        "/services/{service_name}",
        response_model=ServiceHealthResponse,
        responses={404: {"model": ErrorResponse}},
        summary="Status of a single service",
    )
    def get_service_health(service_name: str) -> ServiceHealthResponse:
        """Status of one service, including last failure details."""
        health = registry.get_health(service_name)
        if health is None:
            raise HTTPException(
                status_code=404, detail=f"Service not found: {service_name}"
            )
        return ServiceHealthResponse(**health.to_dict())

    @router.get(
        "/services/{service_name}/endpoint",
        response_model=RegisteredServiceResponse,
        responses={404: {"model": ErrorResponse}},
        summary="Registration details for a service",
    )
    def get_service_endpoint(service_name: str) -> RegisteredServiceResponse:
        """Where the orchestrator believes this service lives."""
        service = registry.get(service_name)
        if service is None:
            raise HTTPException(
                status_code=404, detail=f"Service not found: {service_name}"
            )
        return RegisteredServiceResponse(**service.to_dict())

    @router.post(
        "/services/{service_name}/recover",
        response_model=RecoveryResponse,
        responses={404: {"model": ErrorResponse}},
        summary="Manually mark a service healthy",
    )
    def recover_service(service_name: str) -> RecoveryResponse:
        """Reset a service's circuit breaker after an operator has fixed it.

        Lets a human restart a service and resume orchestration without
        restarting the workflow or the orchestrator itself: tasks blocked on
        the service are re-queued immediately.
        """
        if not registry.recover(service_name):
            raise HTTPException(
                status_code=404, detail=f"Service not found: {service_name}"
            )
        resumed = registry.resume_blocked_tasks(service_name)
        return RecoveryResponse(
            service_name=service_name,
            status=HEALTHY,
            resumed_tasks=resumed,
            message=(
                f"Circuit breaker reset; service marked healthy. "
                f"{resumed} blocked task(s) re-queued."
            ),
        )

    @router.delete(
        "/services/{service_name}",
        response_model=DeregisterResponse,
        responses={404: {"model": ErrorResponse}},
        summary="Forget a service",
    )
    def deregister_service(service_name: str) -> DeregisterResponse:
        """Remove a service from the registry and drop its health record.

        Services are registered when a workflow is submitted and stay listed
        afterwards, so services of use cases that are no longer running keep
        appearing. This removes one. It is re-registered automatically if
        another workflow uses it again.
        """
        if not registry.deregister(service_name):
            raise HTTPException(
                status_code=404, detail=f"Service not found: {service_name}"
            )
        return DeregisterResponse(
            service_name=service_name,
            removed=True,
            message="Service removed from the registry and its health record dropped",
        )

    return router
