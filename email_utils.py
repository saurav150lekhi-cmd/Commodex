import os
import logging
import urllib.request
import urllib.error
import json

log = logging.getLogger(__name__)

RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
FROM_EMAIL     = os.environ.get("FROM_EMAIL", "onboarding@resend.dev")
APP_URL        = os.environ.get("APP_URL", "https://commodex.io")


def send_email(to, subject, html_body):
    if not RESEND_API_KEY:
        log.warning("RESEND_API_KEY not set — skipping email to %s", to)
        return False
    try:
        payload = json.dumps({
            "from":    f"Commodex <{FROM_EMAIL}>",
            "to":      [to],
            "subject": subject,
            "html":    html_body,
        }).encode()
        req = urllib.request.Request(
            "https://api.resend.com/emails",
            data=payload,
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type":  "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            r.read()
        log.info("Email sent to %s: %s", to, subject)
        return True
    except Exception as e:
        log.error("Failed to send email to %s: %s", to, e)
        return False


def send_verification_email(to, token):
    link = f"{APP_URL}/auth/verify/{token}"
    html = f"""
    <div style="background:#0a0908;color:#d4c4a0;font-family:monospace;padding:40px;max-width:520px;margin:0 auto">
      <div style="font-size:22px;color:#e8d8b0;font-weight:300;margin-bottom:4px">Commodex</div>
      <div style="font-size:9px;color:#c8a870;letter-spacing:3px;margin-bottom:28px">RESEARCH TERMINAL</div>
      <p style="margin-bottom:20px;line-height:1.7;color:#c4b490">Please verify your email address to activate your Commodex account.</p>
      <a href="{link}" style="display:inline-block;background:#c8a870;color:#0a0908;padding:11px 28px;text-decoration:none;font-size:11px;letter-spacing:2px;border-radius:3px">VERIFY EMAIL</a>
      <p style="margin-top:28px;color:#6a5a40;font-size:11px;line-height:1.6">This link expires in 24 hours.<br>If you didn't create a Commodex account, you can ignore this email.</p>
    </div>
    """
    return send_email(to, "Verify your Commodex account", html)


def send_reset_email(to, token):
    link = f"{APP_URL}/app#reset?token={token}"
    html = f"""
    <div style="background:#0a0908;color:#d4c4a0;font-family:monospace;padding:40px;max-width:520px;margin:0 auto">
      <div style="font-size:22px;color:#e8d8b0;font-weight:300;margin-bottom:4px">Commodex</div>
      <div style="font-size:9px;color:#c8a870;letter-spacing:3px;margin-bottom:28px">RESEARCH TERMINAL</div>
      <p style="margin-bottom:20px;line-height:1.7;color:#c4b490">We received a request to reset your password. Click the button below to set a new password.</p>
      <a href="{link}" style="display:inline-block;background:#c8a870;color:#0a0908;padding:11px 28px;text-decoration:none;font-size:11px;letter-spacing:2px;border-radius:3px">RESET PASSWORD</a>
      <p style="margin-top:28px;color:#6a5a40;font-size:11px;line-height:1.6">This link expires in 1 hour.<br>If you didn't request a password reset, you can safely ignore this email.</p>
    </div>
    """
    return send_email(to, "Reset your Commodex password", html)


_SENTIMENT_LABELS = {
    "STRONG_BULLISH": "Strong Bullish",
    "BULLISH":        "Bullish",
    "NEUTRAL":        "Neutral",
    "BEARISH":        "Bearish",
    "STRONG_BEARISH": "Strong Bearish",
}
_SENTIMENT_COLORS = {
    "STRONG_BULLISH": "#22c55e",
    "BULLISH":        "#86efac",
    "NEUTRAL":        "#fbbf24",
    "BEARISH":        "#f87171",
    "STRONG_BEARISH": "#ef4444",
}
_COMMODITY_COLORS = {
    "Gold":        "#c9a84c",
    "Silver":      "#8faabf",
    "Crude Oil":   "#b85c38",
    "Copper":      "#b87040",
    "Natural Gas": "#5a9e8f",
    "Corn":        "#e8c84a",
    "Wheat":       "#d4a843",
    "Soybeans":    "#8db56e",
    "Coffee":      "#8b5e3c",
    "Sugar":       "#d4748a",
}


def send_analysis_notification_email(to, summaries):
    rows = ""
    for commodity, sentiment in summaries.items():
        color      = _COMMODITY_COLORS.get(commodity, "#c8a870")
        sent_label = _SENTIMENT_LABELS.get(sentiment, sentiment)
        sent_color = _SENTIMENT_COLORS.get(sentiment, "#fbbf24")
        rows += f"""
        <tr>
          <td style="padding:10px 16px;border-bottom:1px solid #1e1c18;font-size:11px;letter-spacing:1px;color:{color}">{commodity.upper()}</td>
          <td style="padding:10px 16px;border-bottom:1px solid #1e1c18;font-size:11px;color:{sent_color};letter-spacing:1px">{sent_label.upper()}</td>
        </tr>"""
    link = f"{APP_URL}/app"
    html = f"""
    <div style="background:#0a0908;color:#d4c4a0;font-family:monospace;padding:40px;max-width:560px;margin:0 auto">
      <div style="font-size:22px;color:#e8d8b0;font-weight:300;margin-bottom:4px">Commodex</div>
      <div style="font-size:9px;color:#c8a870;letter-spacing:3px;margin-bottom:28px">RESEARCH TERMINAL</div>
      <div style="font-size:11px;letter-spacing:2px;color:#c8a870;margin-bottom:16px">NEW ANALYSIS READY</div>
      <table style="width:100%;border-collapse:collapse;border:1px solid #1e1c18;margin-bottom:24px">
        <thead>
          <tr style="background:#0d0c0a">
            <th style="padding:8px 16px;text-align:left;font-size:9px;letter-spacing:2px;color:#6a5a40;font-weight:normal">COMMODITY</th>
            <th style="padding:8px 16px;text-align:left;font-size:9px;letter-spacing:2px;color:#6a5a40;font-weight:normal">SENTIMENT</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
      <a href="{link}" style="display:inline-block;background:#c8a870;color:#0a0908;padding:11px 28px;text-decoration:none;font-size:11px;letter-spacing:2px">OPEN TERMINAL</a>
      <p style="margin-top:28px;color:#6a5a40;font-size:10px;line-height:1.6">
        You're receiving this because you enabled analysis notifications.<br>
        To unsubscribe, open the Commodex terminal and toggle off notifications.
      </p>
    </div>
    """
    return send_email(to, "Commodex · New analysis ready", html)


def send_alert_email(to, commodity, new_sentiment, old_sentiment, summary=""):
    comm_color    = _COMMODITY_COLORS.get(commodity, "#c8a870")
    new_label     = _SENTIMENT_LABELS.get(new_sentiment, new_sentiment)
    old_label     = _SENTIMENT_LABELS.get(old_sentiment, old_sentiment)
    new_color     = _SENTIMENT_COLORS.get(new_sentiment, "#fbbf24")
    old_color     = _SENTIMENT_COLORS.get(old_sentiment, "#fbbf24")
    link          = f"{APP_URL}/app"
    summary_block = f'<p style="margin:16px 0 0;line-height:1.7;color:#c4b490;font-size:12px">{summary}</p>' if summary else ""
    html = f"""
    <div style="background:#0a0908;color:#d4c4a0;font-family:monospace;padding:40px;max-width:560px;margin:0 auto">
      <div style="font-size:22px;color:#e8d8b0;font-weight:300;margin-bottom:4px">Commodex</div>
      <div style="font-size:9px;color:#c8a870;letter-spacing:3px;margin-bottom:28px">RESEARCH TERMINAL</div>
      <div style="font-size:11px;letter-spacing:2px;color:{comm_color};margin-bottom:12px">{commodity.upper()} · SENTIMENT ALERT</div>
      <div style="background:#0d0c0a;border:1px solid #2a2820;border-left:3px solid {comm_color};padding:16px 20px;margin-bottom:20px">
        <div style="display:flex;align-items:center;gap:16px">
          <div style="text-align:center">
            <div style="font-size:9px;color:#6a5a40;letter-spacing:1px;margin-bottom:4px">PREVIOUS</div>
            <div style="font-size:13px;color:{old_color};letter-spacing:1px">{old_label.upper()}</div>
          </div>
          <div style="color:#3a3428;font-size:18px">→</div>
          <div style="text-align:center">
            <div style="font-size:9px;color:#6a5a40;letter-spacing:1px;margin-bottom:4px">NOW</div>
            <div style="font-size:16px;font-weight:bold;color:{new_color};letter-spacing:1px">{new_label.upper()}</div>
          </div>
        </div>
        {summary_block}
      </div>
      <a href="{link}" style="display:inline-block;background:#c8a870;color:#0a0908;padding:11px 28px;text-decoration:none;font-size:11px;letter-spacing:2px">OPEN TERMINAL</a>
      <p style="margin-top:28px;color:#6a5a40;font-size:10px;line-height:1.6">
        You're receiving this because you subscribed to {commodity} alerts.<br>
        To manage alerts, open the Commodex terminal and go to Settings.
      </p>
    </div>
    """
    subject = f"Commodex · {commodity} sentiment shifted to {new_label}"
    return send_email(to, subject, html)
