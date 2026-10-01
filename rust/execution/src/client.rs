use std::{
    collections::BTreeMap,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use reqwest::{Client, Method};
use risk::{RiskEngine, RiskError};
use rust_decimal::Decimal;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use tokio::time::sleep;
use tracing::warn;

use crate::{
    auth::sign_hmac_sha256,
    model::{parse_decimal, terminal_status},
    BalanceEntry, BalanceSnapshot, CancelAck, ExecutionAttemptError, ExecutionConfig,
    ExecutionError, ExecutionFill, ExecutionMode, ExecutionOrderRequest, ExecutionResult,
    ExecutionStage, MarketUnit, MonitorResult, OrderExecutionState, OrderSide, OrderType,
    PlaceOrderAck, PreparedExecution, TimeInForce,
};

#[derive(Clone)]
pub struct BybitExecutionClient {
    config: ExecutionConfig,
    http: Client,
}

impl BybitExecutionClient {
    pub fn new(config: ExecutionConfig) -> Result<Self, ExecutionError> {
        config.validate()?;
        let http = Client::builder()
            .timeout(config.request_timeout)
            .build()
            .map_err(|error| ExecutionError::InvalidConfig(error.to_string()))?;
        Ok(Self { config, http })
    }

    pub fn config(&self) -> &ExecutionConfig {
        &self.config
    }

    pub async fn place_order(
        &self,
        risk_engine: &mut RiskEngine,
        prepared: &PreparedExecution,
        request: &ExecutionOrderRequest,
    ) -> Result<PlaceOrderAck, ExecutionError> {
        self.validate_execution_environment(prepared)?;
        risk_engine
            .validate_approval(
                &prepared.approval,
                prepared.trade_id(),
                current_time_ms(),
            )
            .map_err(risk_state_error)?;
        request.validate(self.config.max_order_notional)?;

        let body = PlaceOrderBody::from_request(request);
        let body_json = serde_json::to_string(&body)
            .map_err(|error| ExecutionError::Decode(error.to_string()))?;

        let mut last_error: Option<ExecutionError> = None;
        for attempt in 0..=self.config.max_retries {
            if attempt > 0 {
                if let Err(error) = risk_engine.validate_approval(
                    &prepared.approval,
                    prepared.trade_id(),
                    current_time_ms(),
                ) {
                    let gate_error = risk_state_error(error);
                    return Err(last_error.unwrap_or(gate_error));
                }
            }
            match self
                .private_post_once::<PlaceOrderResult>("/v5/order/create", &body_json)
                .await
            {
                Ok((result, time)) => {
                    return Ok(PlaceOrderAck {
                        order_id: result.order_id,
                        order_link_id: result.order_link_id,
                        accepted_at_ms: time,
                    });
                }
                Err(error) => {
                    let ambiguous = error.is_retryable() || error.is_duplicate_request();
                    if ambiguous {
                        if let Ok(Some(order)) = self
                            .get_order_by_link_id(&request.symbol, &request.order_link_id)
                            .await
                        {
                            return Ok(PlaceOrderAck {
                                order_id: order.order_id,
                                order_link_id: order.order_link_id,
                                accepted_at_ms: order.created_time,
                            });
                        }
                    }

                    if error.is_duplicate_request() {
                        sleep(self.config.poll_interval).await;
                        if let Ok(Some(order)) = self
                            .get_order_by_link_id(&request.symbol, &request.order_link_id)
                            .await
                        {
                            return Ok(PlaceOrderAck {
                                order_id: order.order_id,
                                order_link_id: order.order_link_id,
                                accepted_at_ms: order.created_time,
                            });
                        }
                        return Err(error);
                    }
                    if !error.is_retryable() || attempt == self.config.max_retries {
                        return Err(error);
                    }
                    warn!(
                        attempt,
                        error = %error,
                        order_link_id = %request.order_link_id,
                        "ambiguous place-order failure; verified no order yet and will retry"
                    );
                    last_error = Some(error);
                    sleep(self.backoff(attempt)).await;
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            ExecutionError::MissingData("place-order retry loop ended unexpectedly".to_string())
        }))
    }

    pub async fn cancel_order(
        &self,
        symbol: &str,
        order_id: &str,
        order_link_id: Option<&str>,
    ) -> Result<CancelAck, ExecutionError> {
        if symbol.trim().is_empty() || order_id.trim().is_empty() {
            return Err(ExecutionError::InvalidOrder(
                "cancel requires non-empty symbol and order_id".to_string(),
            ));
        }

        let body = CancelOrderBody {
            category: "spot",
            symbol,
            order_id,
            order_link_id,
        };
        let body_json = serde_json::to_string(&body)
            .map_err(|error| ExecutionError::Decode(error.to_string()))?;

        for attempt in 0..=self.config.max_retries {
            match self
                .private_post_once::<CancelOrderResult>("/v5/order/cancel", &body_json)
                .await
            {
                Ok((result, time)) => {
                    return Ok(CancelAck {
                        order_id: result.order_id,
                        order_link_id: result.order_link_id,
                        accepted_at_ms: time,
                        already_terminal: false,
                    });
                }
                Err(error) => {
                    if let Ok(Some(order)) = self.get_order_by_id(symbol, order_id).await {
                        if terminal_status(&order.order_status) {
                            return Ok(CancelAck {
                                order_id: order.order_id,
                                order_link_id: order.order_link_id,
                                accepted_at_ms: order.updated_time,
                                already_terminal: true,
                            });
                        }
                    }

                    if !error.is_retryable() || attempt == self.config.max_retries {
                        return Err(error);
                    }
                    sleep(self.backoff(attempt)).await;
                }
            }
        }

        Err(ExecutionError::MissingData(
            "cancel-order retry loop ended unexpectedly".to_string(),
        ))
    }

    pub async fn monitor_order(
        &self,
        symbol: &str,
        order_id: &str,
    ) -> Result<MonitorResult, ExecutionError> {
        self.monitor_order_for(symbol, order_id, self.config.order_timeout)
            .await
    }

    pub async fn sync_balances(
        &self,
        coins: &[String],
    ) -> Result<BalanceSnapshot, ExecutionError> {
        let mut params = vec![("accountType".to_string(), self.config.account_type.clone())];
        if !coins.is_empty() {
            let normalized = coins
                .iter()
                .map(|coin| coin.to_uppercase())
                .collect::<Vec<_>>()
                .join(",");
            params.push(("coin".to_string(), normalized));
        }

        let (result, time) = self
            .private_get::<WalletBalanceResult>("/v5/account/wallet-balance", params)
            .await?;

        let account = result.list.into_iter().next().ok_or_else(|| {
            ExecutionError::MissingData("wallet-balance response contained no account".to_string())
        })?;

        let mut entries = Vec::with_capacity(account.coin.len());
        for coin in account.coin {
            let wallet = parse_decimal("walletBalance", &coin.wallet_balance)?;
            let locked = parse_decimal("locked", &coin.locked)?;
            let borrow = parse_decimal("spotBorrow", &coin.spot_borrow)?;
            let available = (wallet - locked - borrow).max(Decimal::ZERO);
            entries.push(BalanceEntry {
                coin: coin.coin,
                wallet_balance: wallet,
                locked,
                spot_borrow: borrow,
                equity: parse_decimal("equity", &coin.equity)?,
                usd_value: parse_decimal("usdValue", &coin.usd_value)?,
                estimated_spot_available: available,
            });
        }

        Ok(BalanceSnapshot {
            account_type: account.account_type,
            total_equity_usd: parse_decimal("totalEquity", &account.total_equity)?,
            total_wallet_balance_usd: parse_decimal(
                "totalWalletBalance",
                &account.total_wallet_balance,
            )?,
            total_available_balance_usd: parse_decimal(
                "totalAvailableBalance",
                &account.total_available_balance,
            )?,
            coins: entries,
            synchronized_at_ms: time,
        })
    }

    pub async fn execute_with_risk_tracking(
        &self,
        risk_engine: &mut RiskEngine,
        prepared: &PreparedExecution,
        request: &ExecutionOrderRequest,
    ) -> Result<ExecutionResult, ExecutionError> {
        self.execute_with_risk_tracking_detailed(risk_engine, prepared, request)
            .await
            .map_err(|error| error.source)
    }

    pub async fn execute_with_risk_tracking_detailed(
        &self,
        risk_engine: &mut RiskEngine,
        prepared: &PreparedExecution,
        request: &ExecutionOrderRequest,
    ) -> Result<ExecutionResult, ExecutionAttemptError> {
        let place_ack = match self.place_order(risk_engine, prepared, request).await {
            Ok(value) => value,
            Err(error) => {
                if error.counts_as_execution_failure() {
                    self.record_failure(
                        risk_engine,
                        format!("order submission failed: {error}"),
                    )
                    .map_err(|source| ExecutionAttemptError {
                        stage: ExecutionStage::Submission,
                        place_ack: None,
                        source,
                    })?;
                }
                return Err(ExecutionAttemptError {
                    stage: ExecutionStage::Submission,
                    place_ack: None,
                    source: error,
                });
            }
        };

        let mut monitor = match self
            .monitor_order(&request.symbol, &place_ack.order_id)
            .await
        {
            Ok(value) => value,
            Err(error) => {
                self.record_failure(
                    risk_engine,
                    format!("order monitoring failed: {error}"),
                )
                .map_err(|source| ExecutionAttemptError {
                    stage: ExecutionStage::Monitoring,
                    place_ack: Some(place_ack.clone()),
                    source,
                })?;
                return Err(ExecutionAttemptError {
                    stage: ExecutionStage::Monitoring,
                    place_ack: Some(place_ack.clone()),
                    source: error,
                });
            }
        };

        let mut cancellation = None;
        if monitor.timed_out
            && !monitor.state.terminal
            && self.config.cancel_on_timeout
        {
            let ack = match self
                .cancel_order(
                    &request.symbol,
                    &place_ack.order_id,
                    Some(&place_ack.order_link_id),
                )
                .await
            {
                Ok(value) => value,
                Err(error) => {
                    self.record_failure(
                        risk_engine,
                        format!("timeout cancellation failed: {error}"),
                    )
                    .map_err(|source| ExecutionAttemptError {
                        stage: ExecutionStage::Cancellation,
                        place_ack: Some(place_ack.clone()),
                        source,
                    })?;
                    return Err(ExecutionAttemptError {
                        stage: ExecutionStage::Cancellation,
                        place_ack: Some(place_ack.clone()),
                        source: error,
                    });
                }
            };
            cancellation = Some(ack);
            monitor = match self
                .monitor_order_for(
                    &request.symbol,
                    &place_ack.order_id,
                    self.config.cancel_confirmation_timeout,
                )
                .await
            {
                Ok(value) => value,
                Err(error) => {
                    self.record_failure(
                        risk_engine,
                        format!("cancel confirmation failed: {error}"),
                    )
                    .map_err(|source| ExecutionAttemptError {
                        stage: ExecutionStage::CancelConfirmation,
                        place_ack: Some(place_ack.clone()),
                        source,
                    })?;
                    return Err(ExecutionAttemptError {
                        stage: ExecutionStage::CancelConfirmation,
                        place_ack: Some(place_ack.clone()),
                        source: error,
                    });
                }
            };
        }

        if monitor.state.fully_filled && monitor.state.fills_confirmed {
            risk_engine
                .record_execution_success(current_time_ms())
                .map_err(risk_state_error)
                .map_err(|source| ExecutionAttemptError {
                    stage: ExecutionStage::Monitoring,
                    place_ack: Some(place_ack.clone()),
                    source,
                })?;
        } else {
            self.record_failure(
                risk_engine,
                format!(
                    concat!(
                        "order {} ended status={} filled={} remaining={} ",
                        "timed_out={} fills_confirmed={}"
                    ),
                    monitor.state.order_id,
                    monitor.state.status,
                    monitor.state.filled_quantity,
                    monitor.state.remaining_quantity,
                    monitor.timed_out,
                    monitor.state.fills_confirmed
                ),
            )
            .map_err(|source| ExecutionAttemptError {
                stage: ExecutionStage::Monitoring,
                place_ack: Some(place_ack.clone()),
                source,
            })?;
        }

        Ok(ExecutionResult {
            place_ack,
            monitor,
            cancellation,
        })
    }

    async fn monitor_order_for(
        &self,
        symbol: &str,
        order_id: &str,
        timeout: Duration,
    ) -> Result<MonitorResult, ExecutionError> {
        let started = Instant::now();
        loop {
            let order = match self.get_order_by_id(symbol, order_id).await? {
                Some(value) => value,
                None if started.elapsed() < timeout => {
                    sleep(self.config.poll_interval).await;
                    continue;
                }
                None => return Err(ExecutionError::OrderNotFound(order_id.to_string())),
            };

            if terminal_status(&order.order_status) {
                let state = self.build_state(order, true).await?;
                return Ok(MonitorResult {
                    state,
                    timed_out: false,
                });
            }

            if started.elapsed() >= timeout {
                let state = self.build_state(order, false).await?;
                return Ok(MonitorResult {
                    state,
                    timed_out: true,
                });
            }

            sleep(self.config.poll_interval).await;
        }
    }

    async fn build_state(
        &self,
        order: RawOrder,
        confirm_fills: bool,
    ) -> Result<OrderExecutionState, ExecutionError> {
        let target_filled = parse_decimal("cumExecQty", &order.cum_exec_qty)?;
        let started = Instant::now();

        let (fills, fills_confirmed) = loop {
            let fills = self.list_executions(&order.symbol, &order.order_id).await?;
            let total = fills
                .iter()
                .fold(Decimal::ZERO, |acc, fill| acc + fill.quantity);

            let confirmed = total >= target_filled;
            if !confirm_fills
                || target_filled == Decimal::ZERO
                || confirmed
                || started.elapsed() >= self.config.fill_confirmation_timeout
            {
                break (fills, target_filled == Decimal::ZERO || confirmed);
            }
            sleep(self.config.poll_interval).await;
        };

        let mut fees: BTreeMap<String, Decimal> = BTreeMap::new();
        let mut weighted_price = Decimal::ZERO;
        let mut fill_quantity = Decimal::ZERO;
        for fill in &fills {
            fill_quantity += fill.quantity;
            weighted_price += fill.price * fill.quantity;
            let currency = if fill.fee_currency.is_empty() {
                "UNKNOWN".to_string()
            } else {
                fill.fee_currency.clone()
            };
            *fees.entry(currency).or_insert(Decimal::ZERO) += fill.fee;
        }

        let order_filled = target_filled;
        let requested = parse_decimal("qty", &order.qty)?;
        let remaining = parse_decimal("leavesQty", &order.leaves_qty)?;
        let average = if fill_quantity > Decimal::ZERO {
            Some(weighted_price / fill_quantity)
        } else if !order.avg_price.is_empty() {
            Some(parse_decimal("avgPrice", &order.avg_price)?)
        } else {
            None
        };
        let terminal = terminal_status(&order.order_status);
        let fully_filled = order.order_status == "Filled"
            && remaining == Decimal::ZERO
            && order_filled >= requested;

        Ok(OrderExecutionState {
            order_id: order.order_id,
            order_link_id: order.order_link_id,
            symbol: order.symbol,
            status: order.order_status,
            requested_quantity: requested,
            filled_quantity: order_filled,
            remaining_quantity: remaining,
            average_fill_price: average,
            fees,
            fills,
            terminal,
            fully_filled,
            fills_confirmed,
            reject_reason: nonempty(order.reject_reason),
            updated_at_ms: order.updated_time,
        })
    }

    async fn list_executions(
        &self,
        symbol: &str,
        order_id: &str,
    ) -> Result<Vec<ExecutionFill>, ExecutionError> {
        let mut cursor: Option<String> = None;
        let mut output = Vec::new();

        for _ in 0..self.config.max_execution_pages {
            let mut params = vec![
                ("category".to_string(), "spot".to_string()),
                ("symbol".to_string(), symbol.to_string()),
                ("orderId".to_string(), order_id.to_string()),
                ("limit".to_string(), "100".to_string()),
            ];
            if let Some(value) = cursor.as_ref() {
                params.push(("cursor".to_string(), value.clone()));
            }

            let (result, _) = self
                .private_get::<ExecutionListResult>("/v5/execution/list", params)
                .await?;

            for raw in result.list {
                output.push(ExecutionFill {
                    execution_id: raw.exec_id,
                    order_id: raw.order_id,
                    quantity: parse_decimal("execQty", &raw.exec_qty)?,
                    price: parse_decimal("execPrice", &raw.exec_price)?,
                    value: parse_decimal("execValue", &raw.exec_value)?,
                    fee: parse_decimal("execFee", &raw.exec_fee)?,
                    fee_currency: raw.fee_currency,
                    is_maker: raw.is_maker,
                    executed_at_ms: raw.exec_time.parse::<u64>().map_err(|_| {
                        ExecutionError::InvalidNumber {
                            field: "execTime".to_string(),
                            value: raw.exec_time.clone(),
                        }
                    })?,
                });
            }

            if result.next_page_cursor.is_empty() {
                break;
            }
            cursor = Some(result.next_page_cursor);
        }

        output.sort_by(|left, right| {
            left.executed_at_ms
                .cmp(&right.executed_at_ms)
                .then_with(|| left.execution_id.cmp(&right.execution_id))
        });
        output.dedup_by(|left, right| left.execution_id == right.execution_id);
        Ok(output)
    }

    async fn get_order_by_id(
        &self,
        symbol: &str,
        order_id: &str,
    ) -> Result<Option<RawOrder>, ExecutionError> {
        self.query_order(
            symbol,
            vec![("orderId".to_string(), order_id.to_string())],
        )
        .await
    }

    async fn get_order_by_link_id(
        &self,
        symbol: &str,
        order_link_id: &str,
    ) -> Result<Option<RawOrder>, ExecutionError> {
        self.query_order(
            symbol,
            vec![("orderLinkId".to_string(), order_link_id.to_string())],
        )
        .await
    }

    async fn query_order(
        &self,
        symbol: &str,
        identifier: Vec<(String, String)>,
    ) -> Result<Option<RawOrder>, ExecutionError> {
        let mut params = vec![
            ("category".to_string(), "spot".to_string()),
            ("symbol".to_string(), symbol.to_string()),
        ];
        params.extend(identifier.clone());

        let (realtime, _) = self
            .private_get::<OrderListResult>("/v5/order/realtime", params)
            .await?;
        if let Some(order) = realtime.list.into_iter().next() {
            return Ok(Some(order));
        }

        let mut history_params = vec![
            ("category".to_string(), "spot".to_string()),
            ("symbol".to_string(), symbol.to_string()),
        ];
        history_params.extend(identifier);
        let (history, _) = self
            .private_get::<OrderListResult>("/v5/order/history", history_params)
            .await?;
        Ok(history.list.into_iter().next())
    }

    fn validate_execution_environment(
        &self,
        prepared: &PreparedExecution,
    ) -> Result<(), ExecutionError> {
        match (self.config.testnet, prepared.mode()) {
            (true, ExecutionMode::Testnet) => Ok(()),
            (false, ExecutionMode::Live) if self.config.live_trading_enabled => Ok(()),
            (true, mode) => Err(ExecutionError::EnvironmentMismatch(format!(
                "testnet client requires ExecutionMode::Testnet, received {mode:?}"
            ))),
            (false, mode) => Err(ExecutionError::EnvironmentMismatch(format!(
                "mainnet client requires enabled ExecutionMode::Live, received {mode:?}"
            ))),
        }
    }

    async fn private_get<T>(
        &self,
        path: &str,
        params: Vec<(String, String)>,
    ) -> Result<(T, u64), ExecutionError>
    where
        T: DeserializeOwned,
    {
        let mut params = params;
        params.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
        let query = serde_urlencoded::to_string(&params)
            .map_err(|error| ExecutionError::Decode(error.to_string()))?;

        for attempt in 0..=self.config.max_retries {
            match self.private_request_once(Method::GET, path, &query).await {
                Ok(value) => return decode_result(value),
                Err(error) if error.is_retryable() && attempt < self.config.max_retries => {
                    sleep(self.backoff(attempt)).await;
                }
                Err(error) => return Err(error),
            }
        }

        Err(ExecutionError::MissingData(
            "GET retry loop ended unexpectedly".to_string(),
        ))
    }

    async fn private_post_once<T>(
        &self,
        path: &str,
        body: &str,
    ) -> Result<(T, u64), ExecutionError>
    where
        T: DeserializeOwned,
    {
        let envelope = self.private_request_once(Method::POST, path, body).await?;
        decode_result(envelope)
    }

    async fn private_request_once(
        &self,
        method: Method,
        path: &str,
        payload: &str,
    ) -> Result<ApiEnvelope, ExecutionError> {
        let timestamp = current_time_ms();
        let signature = sign_hmac_sha256(
            self.config.api_secret(),
            timestamp,
            self.config.api_key(),
            self.config.recv_window_ms,
            payload,
        )?;
        let url = if method == Method::GET && !payload.is_empty() {
            format!("{}{}?{}", self.config.base_url(), path, payload)
        } else {
            format!("{}{}", self.config.base_url(), path)
        };

        let mut request = self
            .http
            .request(method.clone(), &url)
            .header("X-BAPI-API-KEY", self.config.api_key())
            .header("X-BAPI-TIMESTAMP", timestamp.to_string())
            .header("X-BAPI-RECV-WINDOW", self.config.recv_window_ms.to_string())
            .header("X-BAPI-SIGN", signature);

        if method == Method::POST {
            request = request
                .header("Content-Type", "application/json")
                .body(payload.to_string());
        }

        let response = request
            .send()
            .await
            .map_err(|error| ExecutionError::Transport(error.to_string()))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|error| ExecutionError::Transport(error.to_string()))?;

        if !status.is_success() {
            return Err(ExecutionError::HttpStatus {
                status: status.as_u16(),
                body,
            });
        }

        let envelope: ApiEnvelope = serde_json::from_str(&body)
            .map_err(|error| ExecutionError::Decode(error.to_string()))?;
        if envelope.ret_code != 0 {
            return Err(ExecutionError::Bybit {
                code: envelope.ret_code,
                message: envelope.ret_msg,
            });
        }
        Ok(envelope)
    }

    fn backoff(&self, attempt: u32) -> Duration {
        let factor = 1_u32 << attempt.min(10);
        self.config.retry_base_delay.saturating_mul(factor)
    }

    fn record_failure(
        &self,
        risk_engine: &mut RiskEngine,
        detail: String,
    ) -> Result<(), ExecutionError> {
        risk_engine
            .record_execution_failure(current_time_ms(), detail)
            .map_err(risk_state_error)?;
        Ok(())
    }
}

fn risk_state_error(error: RiskError) -> ExecutionError {
    ExecutionError::RiskState(error.to_string())
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn decode_result<T: DeserializeOwned>(
    envelope: ApiEnvelope,
) -> Result<(T, u64), ExecutionError> {
    let result = serde_json::from_value::<T>(envelope.result)
        .map_err(|error| ExecutionError::Decode(error.to_string()))?;
    Ok((result, envelope.time))
}

fn nonempty(value: String) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

#[derive(Debug, Deserialize)]
struct ApiEnvelope {
    #[serde(rename = "retCode")]
    ret_code: i64,
    #[serde(rename = "retMsg")]
    ret_msg: String,
    result: Value,
    time: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PlaceOrderBody<'a> {
    category: &'static str,
    symbol: &'a str,
    side: OrderSide,
    order_type: OrderType,
    qty: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    price: Option<String>,
    time_in_force: TimeInForce,
    order_link_id: &'a str,
    is_leverage: u8,
    order_filter: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    market_unit: Option<MarketUnit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    slippage_tolerance_type: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    slippage_tolerance: Option<String>,
}

impl<'a> PlaceOrderBody<'a> {
    fn from_request(request: &'a ExecutionOrderRequest) -> Self {
        let market = request.order_type == OrderType::Market;
        Self {
            category: "spot",
            symbol: &request.symbol,
            side: request.side,
            order_type: request.order_type,
            qty: request.requested_quantity.normalize().to_string(),
            price: request.price.map(|value| value.normalize().to_string()),
            time_in_force: request.time_in_force,
            order_link_id: &request.order_link_id,
            is_leverage: 0,
            order_filter: "Order",
            market_unit: market.then_some(MarketUnit::BaseCoin),
            slippage_tolerance_type: request
                .slippage_tolerance_percent
                .map(|_| "Percent"),
            slippage_tolerance: request
                .slippage_tolerance_percent
                .map(|value| value.normalize().to_string()),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CancelOrderBody<'a> {
    category: &'static str,
    symbol: &'a str,
    order_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    order_link_id: Option<&'a str>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlaceOrderResult {
    order_id: String,
    order_link_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CancelOrderResult {
    order_id: String,
    order_link_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderListResult {
    #[serde(default)]
    list: Vec<RawOrder>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawOrder {
    symbol: String,
    order_id: String,
    #[serde(default)]
    order_link_id: String,
    #[serde(default)]
    qty: String,
    #[serde(default)]
    cum_exec_qty: String,
    #[serde(default)]
    leaves_qty: String,
    #[serde(default)]
    avg_price: String,
    order_status: String,
    #[serde(default)]
    reject_reason: String,
    #[serde(default, deserialize_with = "deserialize_u64_string_or_number")]
    created_time: u64,
    #[serde(default, deserialize_with = "deserialize_u64_string_or_number")]
    updated_time: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExecutionListResult {
    #[serde(default)]
    list: Vec<RawExecution>,
    #[serde(default)]
    next_page_cursor: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawExecution {
    exec_id: String,
    order_id: String,
    exec_price: String,
    exec_qty: String,
    exec_value: String,
    exec_fee: String,
    #[serde(default)]
    fee_currency: String,
    #[serde(default)]
    is_maker: bool,
    exec_time: String,
}

#[derive(Debug, Deserialize)]
struct WalletBalanceResult {
    #[serde(default)]
    list: Vec<RawWalletAccount>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawWalletAccount {
    account_type: String,
    #[serde(default)]
    total_equity: String,
    #[serde(default)]
    total_wallet_balance: String,
    #[serde(default)]
    total_available_balance: String,
    #[serde(default)]
    coin: Vec<RawWalletCoin>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawWalletCoin {
    coin: String,
    #[serde(default)]
    equity: String,
    #[serde(default)]
    usd_value: String,
    #[serde(default)]
    wallet_balance: String,
    #[serde(default)]
    locked: String,
    #[serde(default)]
    spot_borrow: String,
}

fn deserialize_u64_string_or_number<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::String(text) => text.parse::<u64>().map_err(serde::de::Error::custom),
        Value::Number(number) => number.as_u64().ok_or_else(|| {
            serde::de::Error::custom("timestamp number is not an unsigned integer")
        }),
        Value::Null => Ok(0),
        other => Err(serde::de::Error::custom(format!(
            "unexpected timestamp value {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(value: &str) -> Decimal {
        Decimal::from_str_exact(value).unwrap()
    }

    #[test]
    fn serializes_market_order_with_explicit_base_coin_unit() {
        let request = ExecutionOrderRequest {
            symbol: "BTCUSDT".to_string(),
            side: OrderSide::Buy,
            order_type: OrderType::Market,
            requested_quantity: d("0.0001"),
            estimated_notional: d("4"),
            price: None,
            time_in_force: TimeInForce::Ioc,
            order_link_id: "phase10-test".to_string(),
            slippage_tolerance_percent: Some(d("0.10")),
        };
        let body = serde_json::to_value(PlaceOrderBody::from_request(&request)).unwrap();

        assert_eq!(body["category"], "spot");
        assert_eq!(body["marketUnit"], "baseCoin");
        assert_eq!(body["slippageToleranceType"], "Percent");
        assert_eq!(body["qty"], "0.0001");
    }

    #[test]
    fn identifies_retryable_bybit_errors_without_retrying_bad_parameters() {
        assert!(ExecutionError::Bybit {
            code: 10006,
            message: "rate limit".to_string(),
        }
        .is_retryable());
        assert!(!ExecutionError::Bybit {
            code: 10001,
            message: "bad parameter".to_string(),
        }
        .is_retryable());
    }
}
