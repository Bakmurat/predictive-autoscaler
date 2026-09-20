"""
VictoriaMetrics collector for the predictive autoscaler.
Collects CPU and memory usage metrics from VictoriaMetrics.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List
import requests
import pandas as pd
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _floor_to_grid(dt, seconds):
    """Floor a naive UTC datetime to a multiple of `seconds` since the epoch."""
    from datetime import timedelta as _td
    epoch = int((dt.replace(tzinfo=None) - datetime(1970, 1, 1)).total_seconds())
    return datetime(1970, 1, 1) + _td(seconds=(epoch // seconds) * seconds)



class VictoriaMetricsCollector:
    """Collects CPU and memory metrics from VictoriaMetrics for LSTM training/prediction."""

    def __init__(self, victoria_metrics_url: str = None):
        """
        Initialize VictoriaMetrics collector.
        
        Args:
            victoria_metrics_url: URL of VictoriaMetrics instance (e.g., http://victoriametrics:8428)
        """
        self.vm_url = victoria_metrics_url or os.getenv("VICTORIA_METRICS_URL", "http://victoriametrics:8428")
        self.session = requests.Session()
        self.session.timeout = 30
        logger.info(f"VictoriaMetrics collector initialized with URL: {self.vm_url}")

    def query_range(
        self,
        query: str,
        start_time: datetime,
        end_time: datetime,
        step: str = "5m"
    ) -> Dict:
        """Execute a VictoriaMetrics range query."""
        try:
            params = {
                "query": query,
                "start": int(start_time.timestamp()),
                "end": int(end_time.timestamp()),
                "step": step,
            }
            
            response = self.session.get(
                f"{self.vm_url}/api/v1/query_range",
                params=params
            )
            response.raise_for_status()
            result = response.json()
            
            if result.get("status") != "success":
                logger.error(f"Query failed: {result}")
                return {"status": "error", "data": {"result": []}}
            
            return result

        except requests.exceptions.RequestException as e:
            logger.error(f"VictoriaMetrics query failed: {e}")
            return {"status": "error", "data": {"result": []}}

    def get_cpu_usage(
        self,
        namespace: str,
        pod_pattern: str,
        container: str,
        hours: int = 24
    ) -> pd.DataFrame:
        """
        Get aggregated CPU usage across all matching pods (in cores).
        
        Query: sum(rate(container_cpu_usage_seconds_total[1m]))
        This gives total CPU cores used across all pods (e.g., 0.462 = 462 millicores)
        
        Args:
            namespace: Kubernetes namespace
            pod_pattern: Pod name pattern (e.g., "nginx-test.*")
            container: Container name
            hours: Hours of historical data to fetch
        
        Returns:
            DataFrame with columns: timestamp, value
        """
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)

        query = f"""
        sum(rate(container_cpu_usage_seconds_total{{
            namespace="{namespace}",
            pod=~"{pod_pattern}",
            container="{container}",
            cpu="total"
        }}[1m]))
        """

        # Use 10-min intervals for training data (captures daily patterns efficiently)
        step = "10m" if hours > 24 else "1m"
        logger.info(f"Fetching CPU usage for {namespace}/{pod_pattern}/{container} (step={step}, hours={hours})")
        result = self.query_range(query, start_time, end_time, step=step)

        return self._parse_result_to_dataframe(result)

    def get_memory_usage(
        self,
        namespace: str,
        pod_pattern: str,
        container: str,
        hours: int = 24
    ) -> pd.DataFrame:
        """
        Get aggregated memory usage across all matching pods (working set in bytes).
        
        Query: sum(container_memory_working_set_bytes)
        This gives total active memory across all pods
        
        Args:
            namespace: Kubernetes namespace
            pod_pattern: Pod name pattern
            container: Container name
            hours: Hours of historical data to fetch
        
        Returns:
            DataFrame with columns: timestamp, value
        """
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)

        query = f"""
        sum(container_memory_working_set_bytes{{
            namespace="{namespace}",
            pod=~"{pod_pattern}",
            container="{container}"
        }})
        """

        step = "10m" if hours > 24 else "1m"
        logger.info(f"Fetching memory usage for {namespace}/{pod_pattern}/{container} (step={step}, hours={hours})")
        result = self.query_range(query, start_time, end_time, step=step)

        return self._parse_result_to_dataframe(result)

    def get_cpu_usage_percent(
        self,
        namespace: str,
        pod_pattern: str,
        container: str,
        cpu_request_millicores: float,
        hours: int = 24
    ) -> pd.DataFrame:
        """
        Get CPU usage as percentage of request.
        
        Args:
            namespace: Kubernetes namespace
            pod_pattern: Pod name pattern
            container: Container name
            cpu_request_millicores: CPU request in millicores (e.g., 50 for 50m)
            hours: Hours of historical data to fetch
        
        Returns:
            DataFrame with CPU usage percentage
        """
        df = self.get_cpu_usage(namespace, pod_pattern, container, hours)
        
        if df.empty:
            return df
        
        # Convert cores to percentage of request
        # value is in cores, convert to millicores then calculate percentage
        df['value'] = (df['value'] * 1000 / cpu_request_millicores) * 100
        
        return df

    def get_memory_usage_percent(
        self,
        namespace: str,
        pod_pattern: str,
        container: str,
        memory_request_bytes: float,
        hours: int = 24
    ) -> pd.DataFrame:
        """
        Get memory usage as percentage of request.
        
        Args:
            namespace: Kubernetes namespace
            pod_pattern: Pod name pattern
            container: Container name
            memory_request_bytes: Memory request in bytes (e.g., 67108864 for 64Mi)
            hours: Hours of historical data to fetch
        
        Returns:
            DataFrame with memory usage percentage
        """
        df = self.get_memory_usage(namespace, pod_pattern, container, hours)
        
        if df.empty:
            return df
        
        # Calculate percentage of request
        df['value'] = (df['value'] / memory_request_bytes) * 100
        
        return df

    def get_application_metrics(
        self,
        namespace: str,
        pod_pattern: str,
        container: str,
        cpu_request_millicores: float = 50,
        memory_request_mb: float = 64,
        hours: int = 24
    ) -> Dict[str, pd.DataFrame]:
        """
        Get both CPU and memory metrics for an application.
        
        Args:
            namespace: Kubernetes namespace
            pod_pattern: Pod name pattern
            container: Container name
            cpu_request_millicores: CPU request in millicores
            memory_request_mb: Memory request in MB
            hours: Hours of historical data
        
        Returns:
            Dictionary with 'cpu' and 'memory' DataFrames
        """
        logger.info(f"Collecting metrics for {namespace}/{pod_pattern}")

        metrics = {}

        # CPU usage (as percentage)
        memory_request_bytes = memory_request_mb * 1024 * 1024
        
        cpu_df = self.get_cpu_usage_percent(
            namespace, pod_pattern, container, cpu_request_millicores, hours
        )
        if not cpu_df.empty:
            metrics["cpu"] = cpu_df
            logger.info(f"Collected {len(cpu_df)} CPU data points")

        # Memory usage (as percentage)
        memory_df = self.get_memory_usage_percent(
            namespace, pod_pattern, container, memory_request_bytes, hours
        )
        if not memory_df.empty:
            metrics["memory"] = memory_df
            logger.info(f"Collected {len(memory_df)} memory data points")

        return metrics

    def get_istio_request_rate(
        self,
        destination_workload: str,
        namespace: str = None,
        hours: int = 24
    ) -> pd.DataFrame:
        """
        Get Istio request rate (requests per minute) for a workload.
        
        This is a LEADING INDICATOR - traffic increases BEFORE CPU increases.
        Better for predictive autoscaling than CPU metrics.
        
        Query: sum(rate(istio_requests_total{reporter="destination",destination_workload="nginx-test",destination_workload_namespace="<ns>"}[1m])) * 60
        Returns requests per second, multiplied by 60 to get requests per minute
        
        Args:
            destination_workload: Destination workload name (e.g., "nginx-test")
            namespace: Kubernetes namespace (optional filter)
            hours: Hours of historical data to fetch
        
        Returns:
            DataFrame with columns: timestamp, value (requests per minute)
        """
        # Align the window to the ten-minute wall-clock grid so every sample of every run sits on the same
        # slots (training preflight, scoring, and cross-run comparisons all assume this grid).
        end_time = _floor_to_grid(datetime.utcnow(), 600)
        start_time = end_time - timedelta(hours=hours)

        # Build query
        # Canonical request-count definition shared by training, scaling (operator), the KEDA twin,
        # and the scorer: destination-reported requests only (each request is reported by both the
        # client and the server sidecar; counting both doubles the rate).
        filters = ['reporter="destination"', f'destination_workload="{destination_workload}"']
        if namespace:
            filters.append(f'destination_workload_namespace="{namespace}"')
        
        filter_str = ",".join(filters)
        
        query = f"""
        sum(rate(istio_requests_total{{{filter_str}}}[1m])) * 60
        """

        step = "10m" if hours > 24 else "1m"
        logger.info(f"Fetching Istio request rate for {destination_workload} (step={step}, hours={hours})")
        result = self.query_range(query, start_time, end_time, step=step)
        
        df = self._parse_result_to_dataframe(result)
        
        if not df.empty:
            logger.info(f"Collected {len(df)} request rate data points")
            logger.info(f"Request rate range: {df['value'].min():.0f} - {df['value'].max():.0f} req/min")
        
        return df

    def get_istio_request_rate_normalized(
        self,
        destination_workload: str,
        namespace: str = None,
        baseline_rpm: float = 10000,
        hours: int = 24
    ) -> pd.DataFrame:
        """
        Get Istio request rate normalized to percentage of baseline.
        
        This normalizes request rate to a percentage similar to CPU/memory,
        making it compatible with existing LSTM training pipeline.
        
        Args:
            destination_workload: Destination workload name
            namespace: Kubernetes namespace
            baseline_rpm: Baseline requests per minute (e.g., 10000 = 100%)
            hours: Hours of historical data
        
        Returns:
            DataFrame with request rate as percentage of baseline
        """
        df = self.get_istio_request_rate(destination_workload, namespace, hours)
        
        if df.empty:
            return df
        
        # Convert to percentage of baseline
        # e.g., 5000 rpm / 10000 baseline = 50%
        df['value'] = (df['value'] / baseline_rpm) * 100
        
        logger.info(f"Request rate percentage range: {df['value'].min():.1f}% - {df['value'].max():.1f}%")
        
        return df

    def get_application_metrics_with_requests(
        self,
        namespace: str,
        pod_pattern: str,
        container: str,
        workload_name: str = None,
        cpu_request_millicores: float = 50,
        memory_request_mb: float = 64,
        baseline_rpm: float = 10000,
        hours: int = 24,
        include_requests: bool = True
    ) -> Dict[str, pd.DataFrame]:
        """
        Get CPU, memory, AND request rate metrics for an application.
        
        Args:
            namespace: Kubernetes namespace
            pod_pattern: Pod name pattern
            container: Container name
            workload_name: Workload name for Istio metrics (defaults to pod_pattern without .*)
            cpu_request_millicores: CPU request in millicores
            memory_request_mb: Memory request in MB
            baseline_rpm: Baseline requests per minute for normalization
            hours: Hours of historical data
            include_requests: Whether to fetch Istio request metrics
        
        Returns:
            Dictionary with 'cpu', 'memory', and optionally 'requests' DataFrames
        """
        # Get CPU and memory metrics
        metrics = self.get_application_metrics(
            namespace=namespace,
            pod_pattern=pod_pattern,
            container=container,
            cpu_request_millicores=cpu_request_millicores,
            memory_request_mb=memory_request_mb,
            hours=hours
        )
        
        # Add Istio request rate metrics
        if include_requests:
            # Extract workload name from pod pattern if not provided
            if not workload_name:
                workload_name = pod_pattern.replace(".*", "").replace("*", "")
            
            request_df = self.get_istio_request_rate_normalized(
                destination_workload=workload_name,
                namespace=namespace,
                baseline_rpm=baseline_rpm,
                hours=hours
            )
            
            if not request_df.empty:
                metrics["requests"] = request_df
                logger.info(f"Collected {len(request_df)} request rate data points")
        
        return metrics

    def _parse_result_to_dataframe(self, result: Dict) -> pd.DataFrame:
        """Parse VictoriaMetrics query result into DataFrame."""
        data = []

        if result.get("status") != "success":
            return pd.DataFrame()

        result_data = result.get("data", {}).get("result", [])

        for series in result_data:
            values = series.get("values", [])

            for timestamp, value in values:
                try:
                    data.append({
                        "timestamp": pd.to_datetime(timestamp, unit="s"),
                        "value": float(value)
                    })
                except (ValueError, TypeError):
                    continue

        if not data:
            logger.warning("No data points found in query result")
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df = df.sort_values("timestamp").reset_index(drop=True)
        
        # If multiple series, average them
        if len(df) > 0:
            df = df.groupby("timestamp").agg({"value": "mean"}).reset_index()
        
        return df


# Example usage
if __name__ == "__main__":
    # Initialize collector
    collector = VictoriaMetricsCollector("http://your-victoria-metrics:8428")

    # Example: Fetch metrics for the demo-app frontend
    metrics = collector.get_application_metrics(
        namespace="demo-app",
        pod_pattern="demo-app-frontend.*",
        container="frontend",
        cpu_request_millicores=50,  # 50m
        memory_request_mb=64,  # 64Mi
        hours=48  # Last 48 hours
    )

    if "cpu" in metrics:
        print(f"CPU metrics: {len(metrics['cpu'])} points")
        print(metrics['cpu'].head())
        print(f"CPU range: {metrics['cpu']['value'].min():.2f}% - {metrics['cpu']['value'].max():.2f}%")

    if "memory" in metrics:
        print(f"\nMemory metrics: {len(metrics['memory'])} points")
        print(metrics['memory'].head())
        print(f"Memory range: {metrics['memory']['value'].min():.2f}% - {metrics['memory']['value'].max():.2f}%")
