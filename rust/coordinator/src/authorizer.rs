use async_trait::async_trait;
use execution::{prepare_execution, ExecutionMode, PreparedExecution};
use risk::{RiskContext, RiskEngine, ServiceHealth, TradeIntent};
use scanner::TriangleRoute;

use crate::{
    CoordinatorError, Holdings, PlannedOrder,
};

pub struct LegAuthorizationContext<'a> {
    pub trade_id: &'a str,
    pub route: &'a TriangleRoute,
    pub leg_index: usize,
    pub planned: &'a PlannedOrder,
    pub holdings: &'a Holdings,
    pub now_ms: u64,
}

#[async_trait]
pub trait RiskIntentProvider: Send {
    async fn risk_inputs(
        &mut self,
        context: &LegAuthorizationContext<'_>,
    ) -> Result<(TradeIntent, RiskContext), CoordinatorError>;

    async fn emergency_health(
        &mut self,
    ) -> Result<(ServiceHealth, ServiceHealth), CoordinatorError>;
}

#[async_trait]
pub trait RouteRiskAuthorizer: Send {
    async fn authorize_leg(
        &mut self,
        risk_engine: &mut RiskEngine,
        context: &LegAuthorizationContext<'_>,
    ) -> Result<PreparedExecution, CoordinatorError>;

    async fn emergency_health(
        &mut self,
    ) -> Result<(ServiceHealth, ServiceHealth), CoordinatorError>;

    fn mode(&self) -> ExecutionMode;
    fn live_enabled(&self) -> bool;
}

pub struct RiskEngineAuthorizer<P> {
    provider: P,
    mode: ExecutionMode,
    live_enabled: bool,
}

impl<P> RiskEngineAuthorizer<P> {
    pub fn new(provider: P, mode: ExecutionMode, live_enabled: bool) -> Self {
        Self {
            provider,
            mode,
            live_enabled,
        }
    }
}

#[async_trait]
impl<P> RouteRiskAuthorizer for RiskEngineAuthorizer<P>
where
    P: RiskIntentProvider,
{
    async fn authorize_leg(
        &mut self,
        risk_engine: &mut RiskEngine,
        context: &LegAuthorizationContext<'_>,
    ) -> Result<PreparedExecution, CoordinatorError> {
        let (intent, risk_context) = self.provider.risk_inputs(context).await?;
        if intent.trade_id != context.trade_id {
            return Err(CoordinatorError::Risk(
                "risk intent trade_id does not match coordinator trade_id".to_string(),
            ));
        }

        let decision = risk_engine
            .evaluate(&intent, &risk_context, context.now_ms)
            .map_err(|error| CoordinatorError::Risk(error.to_string()))?;
        if !decision.approved {
            let failed = decision
                .checks
                .iter()
                .filter(|check| !check.passed)
                .map(|check| format!("{:?}: {}", check.check, check.detail))
                .collect::<Vec<_>>()
                .join("; ");
            return Err(CoordinatorError::Risk(format!(
                "risk rejected leg {}: {}",
                context.leg_index + 1,
                failed
            )));
        }

        let approval = decision.into_approval().ok_or_else(|| {
            CoordinatorError::Risk("approved decision had no approval token".to_string())
        })?;
        prepare_execution(
            risk_engine,
            context.trade_id,
            approval,
            context.now_ms,
            self.mode,
            self.live_enabled,
        )
        .map_err(|error| CoordinatorError::Risk(error.to_string()))
    }

    async fn emergency_health(
        &mut self,
    ) -> Result<(ServiceHealth, ServiceHealth), CoordinatorError> {
        self.provider.emergency_health().await
    }

    fn mode(&self) -> ExecutionMode {
        self.mode
    }

    fn live_enabled(&self) -> bool {
        self.live_enabled
    }
}
