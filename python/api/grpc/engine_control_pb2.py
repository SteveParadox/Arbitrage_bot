# Generated from shared/proto/engine_control.proto.
# Do not edit by hand.
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder

_sym_db = _symbol_database.Default()

DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x14engine_control.proto\x12\x13arbitrage.engine.v1"4\n\x0eControlRequest\x12\x12\n\nrequest_id\x18\x01 \x01(\t\x12\x0e\n\x06reason\x18\x02 \x01(\t"\xa9\x01\n\x13UpdateLimitsRequest\x12\x12\n\nrequest_id\x18\x01 \x01(\t\x12\x18\n\x10min_net_edge_bps\x18\x02 \x01(\t\x12\x18\n\x10max_slippage_bps\x18\x03 \x01(\t\x12\x16\n\x0emax_trade_size\x18\x04 \x01(\t\x12\x1a\n\x12max_total_exposure\x18\x05 \x01(\t\x12\x16\n\x0emax_daily_loss\x18\x06 \x01(\t";\n\x15ReloadStrategyRequest\x12\x12\n\nrequest_id\x18\x01 \x01(\t\x12\x0e\n\x06reason\x18\x02 \x01(\t"#\n\rStatusRequest\x12\x12\n\nrequest_id\x18\x01 \x01(\t"l\n\x0cCommandReply\x12\x10\n\x08accepted\x18\x01 \x01(\x08\x12\x0f\n\x07command\x18\x02 \x01(\t\x12\x12\n\nrequest_id\x18\x03 \x01(\t\x12\x0e\n\x06detail\x18\x04 \x01(\t\x12\x15\n\rapplied_at_ms\x18\x05 \x01(\x03"\xab\x01\n\x0cEngineStatus\x12\x0f\n\x07healthy\x18\x01 \x01(\x08\x12\x17\n\x0fruntime_enabled\x18\x02 \x01(\x08\x12\x16\n\x0econtrol_source\x18\x03 \x01(\t\x12\x17\n\x0fgenerated_at_ms\x18\x04 \x01(\x03\x12\x1b\n\x13strategy_generation\x18\x05 \x01(\t\x12\x13\n\x0blimits_json\x18\x06 \x01(\t\x12\x0e\n\x06detail\x18\x07 \x01(\t2\xd0\x03\n\rEngineControl\x12V\n\x0cStartTrading\x12#.arbitrage.engine.v1.ControlRequest\x1a!.arbitrage.engine.v1.CommandReply\x12U\n\x0bStopTrading\x12#.arbitrage.engine.v1.ControlRequest\x1a!.arbitrage.engine.v1.CommandReply\x12[\n\x0cUpdateLimits\x12(.arbitrage.engine.v1.UpdateLimitsRequest\x1a!.arbitrage.engine.v1.CommandReply\x12_\n\x0eReloadStrategy\x12*.arbitrage.engine.v1.ReloadStrategyRequest\x1a!.arbitrage.engine.v1.CommandReply\x12R\n\tGetStatus\x12".arbitrage.engine.v1.StatusRequest\x1a!.arbitrage.engine.v1.EngineStatusb\x06proto3'
)

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(
    DESCRIPTOR,
    "api.grpc.engine_control_pb2",
    _globals,
)
