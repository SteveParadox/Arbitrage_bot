from __future__ import annotations

import uuid
from dataclasses import dataclass

import grpc

from api.grpc import engine_control_pb2, engine_control_pb2_grpc
from api.settings import settings


class EngineCommandError(RuntimeError):
    def __init__(self, detail: str, code_name: str = "UNKNOWN") -> None:
        super().__init__(detail)
        self.code_name = code_name


@dataclass(frozen=True)
class EngineCommandResult:
    accepted: bool
    command: str
    request_id: str
    detail: str
    applied_at_ms: int


class EngineGrpcClient:
    def __init__(self) -> None:
        self._target = settings.arb_engine_grpc_target
        self._token = settings.arb_engine_grpc_token

    def _metadata(self) -> tuple[tuple[str, str], ...]:
        if len(self._token.encode("utf-8")) < 32:
            raise EngineCommandError(
                "ARB_ENGINE_GRPC_TOKEN must contain at least 32 bytes"
            )
        return (("x-engine-token", self._token),)

    async def _call(self, method_name: str, request):
        try:
            async with grpc.aio.insecure_channel(self._target) as channel:
                stub = engine_control_pb2_grpc.EngineControlStub(channel)
                method = getattr(stub, method_name)
                return await method(
                    request,
                    metadata=self._metadata(),
                    timeout=settings.arb_engine_grpc_timeout_seconds,
                )
        except grpc.aio.AioRpcError as error:
            detail = error.details() or error.code().name
            raise EngineCommandError(
                detail,
                error.code().name,
            ) from error

    async def start(self, reason: str) -> EngineCommandResult:
        request_id = uuid.uuid4().hex
        reply = await self._call(
            "StartTrading",
            engine_control_pb2.ControlRequest(
                request_id=request_id,
                reason=reason,
            ),
        )
        return _reply(reply)

    async def stop(self, reason: str) -> EngineCommandResult:
        request_id = uuid.uuid4().hex
        reply = await self._call(
            "StopTrading",
            engine_control_pb2.ControlRequest(
                request_id=request_id,
                reason=reason,
            ),
        )
        return _reply(reply)

    async def update_limits(
        self,
        *,
        min_net_edge_bps: str = "",
        max_slippage_bps: str = "",
        max_trade_size: str = "",
        max_total_exposure: str = "",
        max_daily_loss: str = "",
    ) -> EngineCommandResult:
        request_id = uuid.uuid4().hex
        reply = await self._call(
            "UpdateLimits",
            engine_control_pb2.UpdateLimitsRequest(
                request_id=request_id,
                min_net_edge_bps=min_net_edge_bps,
                max_slippage_bps=max_slippage_bps,
                max_trade_size=max_trade_size,
                max_total_exposure=max_total_exposure,
                max_daily_loss=max_daily_loss,
            ),
        )
        return _reply(reply)

    async def reload_strategy(self, reason: str) -> EngineCommandResult:
        request_id = uuid.uuid4().hex
        reply = await self._call(
            "ReloadStrategy",
            engine_control_pb2.ReloadStrategyRequest(
                request_id=request_id,
                reason=reason,
            ),
        )
        return _reply(reply)

    async def status(self):
        request_id = uuid.uuid4().hex
        return await self._call(
            "GetStatus",
            engine_control_pb2.StatusRequest(request_id=request_id),
        )


def _reply(reply) -> EngineCommandResult:
    return EngineCommandResult(
        accepted=reply.accepted,
        command=reply.command,
        request_id=reply.request_id,
        detail=reply.detail,
        applied_at_ms=reply.applied_at_ms,
    )


engine_grpc_client = EngineGrpcClient()
