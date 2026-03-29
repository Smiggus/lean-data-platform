 """
QuantConnect Custom Data Library
──────────────────────────────────
PythonData subclasses that read from the LEAN data library
produced by the Dagster pipeline.

Drop this file into your algorithm folder alongside main.py.

Classes:
    PipelineEquityData   — daily OHLCV from Databento (via pipeline ZIPs)
    PipelineMinuteData   — minute OHLCV (same format, different directory)
    PipelineFundamentals — FMP fundamentals (fine JSON files)
"""

from AlgorithmImports import *   # noqa — QC runtime injection


class PipelineEquityData(PythonData):
    """
    Reads pipeline-generated daily equity OHLCV ZIPs.

    File: data/equity/usa/daily/{ticker}.zip → {ticker}.csv
    Format (no header): YYYYMMDD HH:MM, open, high, low, close, volume
    Prices in deci-cents — divided by 10,000 on read.
    """

    def get_source(self, config, date, is_live_mode):
        ticker    = config.symbol.value.lower()
        file_path = f"data/equity/usa/daily/{ticker}.zip"
        return SubscriptionDataSource(
            file_path,
            SubscriptionTransportMedium.LOCAL_FILE,
            FileFormat.ZIP,
        )

    def reader(self, config, line, date, is_live_mode):
        if not line or line.startswith("#"):
            return None

        data        = PipelineEquityData()
        data.symbol = config.symbol

        try:
            parts = line.strip().split(",")
            if len(parts) < 6:
                return None

            data.time  = datetime.strptime(parts[0], "%Y%m%d %H:%M")
            data.open  = float(parts[1]) / 10_000
            data.high  = float(parts[2]) / 10_000
            data.low   = float(parts[3]) / 10_000
            data.close = float(parts[4]) / 10_000
            data.value = data.close
            data.volume = int(parts[5])
            data["source"] = "databento_pipeline"
        except Exception:
            return None

        return data


class PipelineMinuteData(PythonData):
    """
    Reads pipeline-generated minute OHLCV ZIPs.
    Register with: self.AddData(PipelineMinuteData, "SPY", Resolution.MINUTE)
    """

    def get_source(self, config, date, is_live_mode):
        ticker    = config.symbol.value.lower()
        date_str  = date.strftime("%Y%m%d")
        file_path = f"data/equity/usa/minute/{ticker}/{date_str}_trade.zip"
        return SubscriptionDataSource(
            file_path,
            SubscriptionTransportMedium.LOCAL_FILE,
            FileFormat.ZIP,
        )

    def reader(self, config, line, date, is_live_mode):
        if not line or line.startswith("#"):
            return None

        data        = PipelineMinuteData()
        data.symbol = config.symbol

        try:
            parts = line.strip().split(",")
            if len(parts) < 6:
                return None

            data.time   = datetime.strptime(parts[0], "%Y%m%d %H:%M")
            data.open   = float(parts[1]) / 10_000
            data.high   = float(parts[2]) / 10_000
            data.low    = float(parts[3]) / 10_000
            data.close  = float(parts[4]) / 10_000
            data.value  = data.close
            data.volume = int(parts[5])
            data["source"] = "databento_pipeline_minute"
        except Exception:
            return None

        return data


class PipelineFundamentals(PythonData):
    """
    Reads pipeline-generated fundamental JSON files.

    File: data/fundamental/fine/{ticker}/{YYYYMMDD}.json
    Exposes income, balance sheet, cash flow, and valuation fields.
    """

    def get_source(self, config, date, is_live_mode):
        ticker    = config.symbol.value.lower()
        date_str  = date.strftime("%Y%m%d")
        file_path = f"data/fundamental/fine/{ticker}/{date_str}.json"
        return SubscriptionDataSource(
            file_path,
            SubscriptionTransportMedium.LOCAL_FILE,
            FileFormat.COLLECTION,
        )

    def reader(self, config, line, date, is_live_mode):
        if not line or line.startswith("#"):
            return None

        import json as _json

        data        = PipelineFundamentals()
        data.symbol = config.symbol
        data.time   = date
        data.value  = 0

        try:
            obj = _json.loads(line)

            income   = obj.get("FinancialStatements", {}).get("IncomeStatement",   {})
            balance  = obj.get("FinancialStatements", {}).get("BalanceSheet",       {})
            cashflow = obj.get("FinancialStatements", {}).get("CashFlowStatement", {})
            val      = obj.get("ValuationRatios", {})
            earn     = obj.get("EarningReports",  {})

            data["revenue"]          = income.get("TotalRevenue")
            data["gross_profit"]     = income.get("GrossProfit")
            data["ebitda"]           = income.get("Ebitda")
            data["net_income"]       = income.get("NetIncome")
            data["operating_income"] = income.get("OperatingIncome")

            data["total_assets"]      = balance.get("TotalAssets")
            data["total_liabilities"] = balance.get("TotalLiabilitiesNetMinorityInterest")
            data["equity"]            = balance.get("CommonStockEquity")
            data["cash"]              = balance.get("CashAndCashEquivalents")
            data["total_debt"]        = balance.get("TotalDebt")

            data["operating_cash_flow"] = cashflow.get("OperatingCashFlow")
            data["capex"]               = cashflow.get("CapitalExpenditure")
            data["free_cash_flow"]      = cashflow.get("FreeCashFlow")

            data["pe_ratio"]    = val.get("PERatio")
            data["pb_ratio"]    = val.get("PBRatio")
            data["ev_ebitda"]   = val.get("EVToEBITDA")
            data["debt_equity"] = val.get("DebtToEquityRatio")
            data["roe"]         = val.get("ReturnOnEquity")
            data["roa"]         = val.get("ReturnOnAssets")

            data["eps"]         = earn.get("BasicEPS")
            data["eps_diluted"] = earn.get("DilutedEPS")

            data["period"]    = obj.get("Period", "annual")
            data["file_date"] = obj.get("FileDate", "")

        except Exception:
            return None

        return data
