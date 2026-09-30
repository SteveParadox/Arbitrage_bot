use async_trait::async_trait;
use execution::{
    BybitExecutionClient, ExecutionAttemptError, ExecutionOrderRequest, ExecutionResult,
    PreparedExecution,
};
use risk::RiskEngine;

#[async_trait]
pub trait CoordinatorVenue: Send + Sync {
    async fn execute(
        &self,
        risk_engine: &mut RiskEngine,
        prepared: &PreparedExecution,
        request: &ExecutionOrderRequest,
    ) -> Result<ExecutionResult, ExecutionAttemptError>;
}

#[async_trait]
impl CoordinatorVenue for BybitExecutionClient {
    async fn execute(
        &self,
        risk_engine: &mut RiskEngine,
        prepared: &PreparedExecution,
        request: &ExecutionOrderRequest,
    ) -> Result<ExecutionResult, ExecutionAttemptError> {
        self.execute_with_risk_tracking_detailed(risk_engine, prepared, request)
            .await
    }
}
