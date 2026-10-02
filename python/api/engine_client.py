from __future__ import annotations

import asyncio
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
        timeout = settings.arb_engine_grpc_timeout_seconds
        if timeout <= 0:
            raise EngineCommandError(
                "ARB_ENGINE_GRPC_TIMEOUT_SECONDS must be greater than zero",
                "INVALID_CONFIGURATION",
            )
        try:
            async with grpc.aio.insecure_channel(self._target) as channel:
                stub = engine_control_pb2_grpc.EngineControlStub(channel)
                method = getattr(stub, method_name)
                return await method(
                    request,
                    metadata=self._metadata(),
                    timeout=timeout,
                )
        except grpc.aio.AioRpcError as error:
            detail = error.details() or error.code().name
            raise EngineCommandError(
                detail,
                error.code().name,
            ) from error

    async def _command_call(self, method_name: str, request):
        max_retries = settings.arb_engine_grpc_max_retries
        initial = settings.arb_engine_grpc_retry_initial_seconds
        maximum = settings.arb_engine_grpc_retry_max_seconds
        if max_retries < 0 or initial <= 0 or maximum <= 0 or initial > maximum:
            raise EngineCommandError(
                "invalid gRPC retry configuration",
                "INVALID_CONFIGURATION",
            )

        retryable = {"UNAVAILABLE", "DEADLINE_EXCEEDED", "CANCELLED"}
        attempt = 0
        while True:
            try:
                return await self._call(method_name, request)
            except EngineCommandError as error:
                if error.code_name not in retryable or attempt >= max_retries:
                    raise
                delay = min(initial * (2**attempt), maximum)
                attempt += 1
                await asyncio.sleep(delay)

    async def start(
        self,
        reason: str,
        *,
        request_id: str | None = None,
    ) -> EngineCommandResult:
        request_id = request_id or uuid.uuid4().hex
        request = engine_control_pb2.ControlRequest(
            request_id=request_id,
            reason=reason,
        )
        reply = await self._command_call(
            "StartTrading",
            request,
        )
        return _reply(
            reply,
            expected_command="start_trading",
            request_id=request_id,
        )

    async def stop(
        self,
        reason: str,
        *,
        request_id: str | None = None,
    ) -> EngineCommandResult:
        request_id = request_id or uuid.uuid4().hex
        request = engine_control_pb2.ControlRequest(
            request_id=request_id,
            reason=reason,
        )
        reply = await self._command_call("StopTrading", request)
        return _reply(
            reply,
            expected_command="stop_trading",
            request_id=request_id,
        )

    async def update_limits(
        self,
        *,
        min_net_edge_bps: str = "",
        max_slippage_bps: str = "",
        max_trade_size: str = "",
        max_total_exposure: str = "",
        max_daily_loss: str = "",
        request_id: str | None = None,
    ) -> EngineCommandResult:
        request_id = request_id or uuid.uuid4().hex
        request = engine_control_pb2.UpdateLimitsRequest(
            request_id=request_id,
            min_net_edge_bps=min_net_edge_bps,
            max_slippage_bps=max_slippage_bps,
            max_trade_size=max_trade_size,
            max_total_exposure=max_total_exposure,
            max_daily_loss=max_daily_loss,
        )
        reply = await self._command_call("UpdateLimits", request)
        return _reply(
            reply,
            expected_command="update_limits",
            request_id=request_id,
        )

    async def reload_strategy(
        self,
        reason: str,
        *,
        request_id: str | None = None,
    ) -> EngineCommandResult:
        request_id = request_id or uuid.uuid4().hex
        request = engine_control_pb2.ReloadStrategyRequest(
            request_id=request_id,
            reason=reason,
        )
        reply = await self._command_call("ReloadStrategy", request)
        return _reply(
            reply,
            expected_command="reload_strategy",
            request_id=request_id,
        )

    async def status(self):
        request_id = uuid.uuid4().hex
        return await self._call(
            "GetStatus",
            engine_control_pb2.StatusRequest(request_id=request_id),
        )


def _reply(
    reply,
    *,
    expected_command: str,
    request_id: str,
) -> EngineCommandResult:
    if reply.request_id != request_id:
        raise EngineCommandError(
            "gRPC command acknowledgement request_id mismatch",
            "INVALID_RESPONSE",
        )
    if reply.command != expected_command:
        raise EngineCommandError(
            (
                "gRPC command acknowledgement mismatch: "
                f"expected {expected_command}, received {reply.command}"
            ),
            "INVALID_RESPONSE",
        )
    if not reply.accepted:
        raise EngineCommandError(
            reply.detail or f"{expected_command} was not accepted",
            "FAILED_PRECONDITION",
        )
    if reply.applied_at_ms <= 0:
        raise EngineCommandError(
            "gRPC command acknowledgement is missing applied_at_ms",
            "INVALID_RESPONSE",
        )

    return EngineCommandResult(
        accepted=True,
        command=reply.command,
        request_id=reply.request_id,
        detail=reply.detail,
        applied_at_ms=reply.applied_at_ms,
    )


engine_grpc_client = EngineGrpcClient()
