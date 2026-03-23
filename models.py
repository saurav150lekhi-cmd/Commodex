import secrets
import hashlib
from datetime import datetime, timezone, timedelta
from db import db


class MarketSignal(db.Model):
    __tablename__ = "market_signals"

    id                 = db.Column(db.Integer, primary_key=True)
    commodity          = db.Column(db.String(50), nullable=False, index=True)
    event              = db.Column(db.Text, nullable=False)
    impact             = db.Column(db.String(20), nullable=False, default="neutral")
    reason             = db.Column(db.Text)
    confidence         = db.Column(db.Integer, default=0)
    source_title       = db.Column(db.Text)
    signal_strength     = db.Column(db.Integer, default=0)
    so_what             = db.Column(db.Text)
    triggered_analysis  = db.Column(db.Boolean, default=False)
    created_at         = db.Column(
        db.DateTime(timezone=True),
        nullable=False,
        index=True,
        default=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        db.Index("ix_market_signals_created_at_desc", "created_at"),
    )


class User(db.Model):
    __tablename__ = "users"

    id                  = db.Column(db.Integer, primary_key=True)
    email               = db.Column(db.String(255), unique=True, nullable=False)
    password_hash       = db.Column(db.String(255), nullable=False)
    is_active           = db.Column(db.Boolean, default=True)
    is_admin            = db.Column(db.Boolean, default=False)
    notify_on_analysis  = db.Column(db.Boolean, default=False)
    email_verified      = db.Column(db.Boolean, default=False)
    verification_token  = db.Column(db.String(64), nullable=True)
    # All JWTs issued before this timestamp are invalidated (set on password reset)
    tokens_valid_after  = db.Column(db.DateTime(timezone=True), nullable=True)
    created_at          = db.Column(db.DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))


class PasswordResetToken(db.Model):
    __tablename__ = "password_reset_tokens"

    id             = db.Column(db.Integer, primary_key=True)
    user_id        = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    # Plaintext token is NEVER stored — only a SHA-256 hash
    token_hash     = db.Column(db.String(64), unique=True, nullable=True)   # hex SHA-256
    token_prefix   = db.Column(db.String(8), nullable=True)                 # first 8 chars for fast lookup
    # Legacy column kept nullable so existing rows aren't broken
    token          = db.Column(db.String(64), nullable=True)
    expires_at     = db.Column(db.DateTime(timezone=True), nullable=False)
    used           = db.Column(db.Boolean, default=False)
    created_by_ip  = db.Column(db.String(45), nullable=True)
    used_by_ip     = db.Column(db.String(45), nullable=True)

    @staticmethod
    def create(user_id, created_by_ip=None):
        """Generate a secure token. Returns (raw_token, model_instance).
        raw_token is sent in the email. Only the hash is stored."""
        raw     = secrets.token_urlsafe(32)
        hashed  = hashlib.sha256(raw.encode()).hexdigest()
        expires = datetime.now(timezone.utc) + timedelta(minutes=30)
        instance = PasswordResetToken(
            user_id       = user_id,
            token_hash    = hashed,
            token_prefix  = raw[:8],
            expires_at    = expires,
            created_by_ip = created_by_ip,
        )
        return raw, instance

    def is_valid(self):
        return not self.used and datetime.now(timezone.utc) < self.expires_at


class UserAlert(db.Model):
    __tablename__ = "user_alerts"

    id        = db.Column(db.Integer, primary_key=True)
    user_id   = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    commodity = db.Column(db.String(50), nullable=False)
    enabled   = db.Column(db.Boolean, default=True)

    __table_args__ = (
        db.UniqueConstraint("user_id", "commodity", name="uq_user_alert"),
    )


class AnalysisRun(db.Model):
    __tablename__ = "analysis_runs"

    id            = db.Column(db.Integer, primary_key=True)
    commodity     = db.Column(db.String(50), nullable=False)
    run_at        = db.Column(db.DateTime(timezone=True), nullable=False)
    data          = db.Column(db.JSON, nullable=False)
    article_count = db.Column(db.Integer)
    sentiment     = db.Column(db.String(20))

    __table_args__ = (
        db.Index("ix_analysis_runs_commodity_run_at", "commodity", run_at.desc()),
    )


class NewsArticle(db.Model):
    __tablename__ = "news_articles"

    id         = db.Column(db.Integer, primary_key=True)
    commodity  = db.Column(db.String(50), nullable=False, index=True)
    title      = db.Column(db.Text, nullable=False)
    url        = db.Column(db.String(500))
    summary    = db.Column(db.Text)
    source     = db.Column(db.String(100))
    published  = db.Column(db.String(50))
    impact     = db.Column(db.String(10), default="LOW")
    fetched_at = db.Column(db.DateTime(timezone=True), nullable=False, index=True)
    url_hash   = db.Column(db.String(64), nullable=False)

    __table_args__ = (
        db.UniqueConstraint("url_hash", "commodity", name="uq_article_commodity"),
    )


class PriceThresholdAlert(db.Model):
    __tablename__ = "price_threshold_alerts"

    id        = db.Column(db.Integer, primary_key=True)
    user_id   = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    commodity = db.Column(db.String(50), nullable=False)
    threshold = db.Column(db.Float, nullable=False)
    direction = db.Column(db.String(5), nullable=False)  # "above" or "below"
    active    = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        db.UniqueConstraint("user_id", "commodity", "direction", name="uq_price_threshold"),
    )


class AriaMemory(db.Model):
    __tablename__ = "aria_memory"

    id         = db.Column(db.Integer, primary_key=True)
    user_id    = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    key        = db.Column(db.String(100), nullable=False)
    value      = db.Column(db.Text, nullable=False)
    updated_at = db.Column(db.DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        db.UniqueConstraint("user_id", "key", name="uq_aria_memory"),
    )


class AriaWatchlist(db.Model):
    __tablename__ = "aria_watchlist"

    id        = db.Column(db.Integer, primary_key=True)
    user_id   = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    commodity = db.Column(db.String(50), nullable=False)
    note      = db.Column(db.Text, nullable=True)
    added_at  = db.Column(db.DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        db.UniqueConstraint("user_id", "commodity", name="uq_aria_watchlist"),
    )
