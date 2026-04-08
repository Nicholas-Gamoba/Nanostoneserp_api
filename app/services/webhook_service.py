# app/services/webhook_service.py
import httpx
import json
import hmac
import hashlib
import logging
import re
from typing import Optional, Dict, Any
from app.config import settings

logger = logging.getLogger(__name__)


class WebhookService:
    def __init__(self):
        self.default_webhook_url = settings.VERCEL_WEBHOOK_URL
        self.webhook_secret = settings.WEBHOOK_SECRET
        logger.info("WebhookService initialized")

    async def send_webhook(
        self,
        result_data: Dict[str, Any],
        origin_url: Optional[str] = None,
        webhook_path: str = "/api/webhook/serp-completed",
    ) -> bool:
        """Send SERP results back to the Vercel app.

        Args:
            result_data: The SERP result payload to send
            origin_url: The origin URL of the incoming request (optional)
            webhook_path: The webhook endpoint path (default: /api/webhook/serp-completed)

        Returns:
            bool: True if webhook sent successfully, False otherwise
        """
        if origin_url:
            webhook_url = self._construct_webhook_url(origin_url, webhook_path)
        else:
            webhook_url = self.default_webhook_url

        if not webhook_url:
            logger.error("No webhook URL available. Cannot send SERP webhook.")
            return False

        logger.info(f"Sending SERP webhook to: {webhook_url}")

        payload = json.dumps(result_data)

        headers = {
            "Content-Type": "application/json",
        }

        if settings.VERCEL_AUTOMATION_BYPASS_SECRET:
            headers["x-vercel-protection-bypass"] = (
                settings.VERCEL_AUTOMATION_BYPASS_SECRET
            )

        if self.webhook_secret:
            signature = self._generate_signature(payload)
            headers["X-Webhook-Signature"] = signature

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    webhook_url,
                    content=payload,
                    headers=headers,
                    timeout=30.0,
                )

                if 200 <= response.status_code < 300:
                    logger.info(
                        f"SERP webhook sent successfully. Status: {response.status_code}"
                    )
                    return True
                else:
                    logger.error(
                        f"SERP webhook failed. Status: {response.status_code}, Body: {response.text}"
                    )
                    return False

            except httpx.TimeoutException as e:
                logger.error(f"SERP webhook timeout: {e}")
                return False
            except httpx.HTTPError as e:
                logger.error(f"HTTP error sending SERP webhook: {e}")
                return False
            except Exception as e:
                logger.error(f"Unexpected error sending SERP webhook: {e}")
                return False

    def _generate_signature(self, payload: str) -> str:
        """Generate HMAC SHA256 signature for the payload."""
        if not self.webhook_secret:
            logger.warning("Webhook secret not set. Cannot generate signature.")
            return ""

        return hmac.new(
            key=self.webhook_secret.encode("utf-8"),
            msg=payload.encode("utf-8"),
            digestmod=hashlib.sha256,
        ).hexdigest()

    def _construct_webhook_url(
        self,
        origin_url: Optional[str],
        webhook_path: str = "/api/webhook/serp-completed",
    ) -> Optional[str]:
        """Construct the webhook URL from the origin URL.

        Args:
            origin_url: The origin URL to construct from
            webhook_path: The webhook endpoint path

        Returns:
            Optional[str]: The constructed webhook URL, or None
        """
        if not origin_url:
            return self.default_webhook_url

        if origin_url.startswith(("http://", "https://")):
            match = re.match(r"^(https?://[^/]+)", origin_url)
            if match:
                origin_url = match.group(1)

        if not origin_url:
            return self.default_webhook_url

        webhook_url = f"{origin_url.rstrip('/')}{webhook_path}"
        logger.info(f"Constructed SERP webhook URL: {webhook_url}")
        return webhook_url
