import smtplib
import os
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

log = logging.getLogger(__name__)

SMTP_HOST  = os.environ.get("SMTP_HOST", "")
SMTP_PORT  = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER  = os.environ.get("SMTP_USER", "")
SMTP_PASS  = os.environ.get("SMTP_PASS", "")
FROM_EMAIL = os.environ.get("FROM_EMAIL", "") or SMTP_USER
APP_URL    = os.environ.get("APP_URL", "http://localhost:5000")


def send_email(to, subject, html_body):
    if not SMTP_HOST or not SMTP_USER:
        log.warning("SMTP not configured — skipping email to %s", to)
        return False
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = f"Commodex <{FROM_EMAIL}>"
        msg["To"]      = to
        msg.attach(MIMEText(html_body, "html"))
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(FROM_EMAIL, to, msg.as_string())
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
