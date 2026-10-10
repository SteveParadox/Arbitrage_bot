# Generated gRPC bindings for engine_control.proto.

from api.grpc import engine_control_pb2 as engine__control__pb2


class EngineControlStub:
    def __init__(self, channel):
        self.StartTrading = channel.unary_unary(
            "/arbitrage.engine.v1.EngineControl/StartTrading",
            request_serializer=engine__control__pb2.ControlRequest.SerializeToString,
            response_deserializer=engine__control__pb2.CommandReply.FromString,
        )
        self.StopTrading = channel.unary_unary(
            "/arbitrage.engine.v1.EngineControl/StopTrading",
            request_serializer=engine__control__pb2.ControlRequest.SerializeToString,
            response_deserializer=engine__control__pb2.CommandReply.FromString,
        )
        self.UpdateLimits = channel.unary_unary(
            "/arbitrage.engine.v1.EngineControl/UpdateLimits",
            request_serializer=engine__control__pb2.UpdateLimitsRequest.SerializeToString,
            response_deserializer=engine__control__pb2.CommandReply.FromString,
        )
        self.ReloadStrategy = channel.unary_unary(
            "/arbitrage.engine.v1.EngineControl/ReloadStrategy",
            request_serializer=engine__control__pb2.ReloadStrategyRequest.SerializeToString,
            response_deserializer=engine__control__pb2.CommandReply.FromString,
        )
        self.GetStatus = channel.unary_unary(
            "/arbitrage.engine.v1.EngineControl/GetStatus",
            request_serializer=engine__control__pb2.StatusRequest.SerializeToString,
            response_deserializer=engine__control__pb2.EngineStatus.FromString,
        )
